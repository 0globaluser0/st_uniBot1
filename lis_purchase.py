# lis_purchase.py
#
# Логика покупки лотов на LIS-SKINS:
#  - /v1/market/buy
#  - /v1/market/info
#  - /v1/market/check-availability
#  - /v1/user/balance
#
# Внешний интерфейс:
#   - LissAccount (dataclass)
#   - load_liss_accounts()          -> List[LissAccount]
#   - init_purchase_dbs(accounts)   -> None
#   - send_lis_request(...)         -> requests.Response  (с обработкой 429)
#   - purchase_lis_skins_for_item(item_name, lots, accounts) -> int
#
# Аккаунты инициализируются в lissbot.py ОДИН РАЗ и дальше передаются сюда.

from __future__ import annotations

import os
import json
import time
import random
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

import config
from priceAnalys import (
    get_cached_item,
    parse_db_timestamp,
    update_purchase_tracking,
)

MAX_IDS_PER_REQUEST = 100

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LISS_ACCS_FILE = os.path.join(BASE_DIR, "liss_accs.txt")

BUY_URL = "https://api.lis-skins.com/v1/market/buy"
INFO_URL = "https://api.lis-skins.com/v1/market/info"
CHECK_AVAIL_URL = "https://api.lis-skins.com/v1/market/check-availability"
BALANCE_URL = "https://api.lis-skins.com/v1/user/balance"


class SkipAccountError(Exception):
    """Сигнал, что текущий аккаунт нужно пропустить и перейти к следующему."""


@dataclass
class LissAccount:
    name: str
    api_key: str
    partner: str
    token: str


@dataclass
class PurchasedLot:
    lot_id: int
    price: float
    status: str
    custom_id: str
    return_reason: Optional[str] = None


# ---------- чтение liss_accs.txt (один раз в lissbot.py) ----------


def load_liss_accounts(path: str = LISS_ACCS_FILE) -> List[LissAccount]:
    """Читаем liss_accs.txt, формат: name_acc@api_key:partner:token."""
    accounts: List[LissAccount] = []

    if not os.path.exists(path):
        raise FileNotFoundError(f"Файл с аккаунтами LIS не найден: {path}")

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            try:
                name_part, rest = raw.split("@", 1)
                api_key, partner, token = rest.split(":", 2)
            except ValueError:
                print(f"[lis_purchase] Некорректная строка в {path}: {raw}")
                continue
            accounts.append(
                LissAccount(
                    name=name_part.strip(),
                    api_key=api_key.strip(),
                    partner=partner.strip(),
                    token=token.strip(),
                )
            )

    if not accounts:
        raise RuntimeError(f"Не удалось распарсить ни одного аккаунта из {path}")

    print(f"[lis_purchase] Загружено {len(accounts)} LIS-аккаунтов:")
    for acc in accounts:
        print(f"  - {acc.name} (partner={acc.partner})")

    return accounts


# ---------- общий HTTP-клиент с обработкой 429 ----------


def send_lis_request(
    method: str,
    url: str,
    *,
    api_key: str,
    params: Optional[Dict[str, Any]] = None,
    json_data: Optional[Dict[str, Any]] = None,
    timeout: Optional[float] = None,
) -> requests.Response:
    """
    Отправка HTTP-запроса к LIS с обработкой 429 (ретраи каждые 2 секунды).
    Таймаут пробрасывается наружу как исключение requests.Timeout.
    """
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    if json_data is not None:
        headers["Content-Type"] = "application/json"

    if timeout is None:
        timeout = getattr(config, "HTTP_TIMEOUT", 20.0)

    while True:
        try:
            resp = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_data,
                timeout=timeout,
            )
        except requests.Timeout:
            # Таймаут обрабатывается на уровне вызывающей функции.
            raise

        if resp.status_code == 429:
            print(f"[lis_purchase] Получен HTTP 429 от {url}, ждём 2 секунды и повторяем...")
            time.sleep(2.0)
            continue

        return resp


def _chunk_list(items: List[int], size: int = MAX_IDS_PER_REQUEST) -> List[List[int]]:
    """Разбивает список на чанки фиксированного размера."""
    return [items[i : i + size] for i in range(0, len(items), size)]


# ---------- БД покупок {account_name}_liss_purchase.db ----------


def _get_purchase_db_conn(account_name: str) -> sqlite3.Connection:
    """
    Возвращает соединение с базой {account_name}_liss_purchase.db
    и гарантирует наличие нужных таблиц.
    """
    db_path = os.path.join(BASE_DIR, f"{account_name}_liss_purchase.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Таблица с покупками (по одной строке на лот)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS purchases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lot_id INTEGER,
            item_name TEXT,
            price REAL,
            unlock_at TEXT,
            created_at TEXT,
            item_float REAL,
            hold_days INTEGER,
            custom_id TEXT,
            status TEXT,
            return_reason TEXT,
            sticker1 TEXT, wear1 REAL,
            sticker2 TEXT, wear2 REAL,
            sticker3 TEXT, wear3 REAL,
            sticker4 TEXT, wear4 REAL,
            sticker5 TEXT, wear5 REAL
        )
        """
    )

    # Таблица со статистикой аккаунта
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS account_stats (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            short_hold_count INTEGER NOT NULL DEFAULT 0,
            min_price_override REAL DEFAULT 0
        )
        """
    )
    cur.execute(
        "INSERT OR IGNORE INTO account_stats(id, short_hold_count, min_price_override) "
        "VALUES(1, 0, 0)"
    )

    conn.commit()
    return conn


def init_purchase_dbs(accounts: List[LissAccount]) -> None:
    """Создаём БД и таблицы для всех аккаунтов (один раз при старте бота)."""
    for acc in accounts:
        conn = _get_purchase_db_conn(acc.name)
        conn.close()


def _get_min_price_override(account_name: str) -> float:
    conn = _get_purchase_db_conn(account_name)
    cur = conn.cursor()
    cur.execute("SELECT min_price_override FROM account_stats WHERE id = 1")
    row = cur.fetchone()
    conn.close()
    if not row:
        return 0.0
    val = row["min_price_override"]
    return float(val) if val is not None else 0.0


def _update_short_hold_stats_and_maybe_min_price(
    account: LissAccount,
    new_short_count: int,
) -> None:
    """
    Обновляем счётчик предметов с холдом 0–7 в БД аккаунта.
    Если суммарное значение > 800 — запрашиваем баланс и считаем
    минимальную цену для будущих покупок этого аккаунта.
    """
    if new_short_count <= 0:
        return

    conn = _get_purchase_db_conn(account.name)
    cur = conn.cursor()
    cur.execute(
        "SELECT short_hold_count, min_price_override FROM account_stats WHERE id = 1"
    )
    row = cur.fetchone()
    current = int(row["short_hold_count"] or 0)
    new_total = current + new_short_count

    cur.execute(
        "UPDATE account_stats SET short_hold_count = ? WHERE id = 1",
        (new_total,),
    )
    conn.commit()

    print(
        f"[lis_purchase][{account.name}] "
        f"Куплено с холдом 0–7: +{new_short_count}, всего={new_total}"
    )

    if new_total > 800:
        print(
            f"[lis_purchase][{account.name}] short_hold_count > 800, "
            f"запрашиваем баланс через /v1/user/balance..."
        )
        try:
            resp = send_lis_request(
                "GET",
                BALANCE_URL,
                api_key=account.api_key,
            )
        except requests.Timeout:
            print(
                f"[lis_purchase][{account.name}] Таймаут при запросе баланса, "
                f"min_price_override не будет изменён."
            )
            conn.close()
            return

        if resp.status_code in (200, 201):
            try:
                payload = resp.json()
            except json.JSONDecodeError:
                print(
                    f"[lis_purchase][{account.name}] Ошибка парсинга JSON баланса, "
                    f"min_price_override не будет изменён."
                )
                conn.close()
                return

            data = payload.get("data") or {}
            balance = data.get("balance")
            try:
                balance_val = float(balance)
            except (TypeError, ValueError):
                print(
                    f"[lis_purchase][{account.name}] Некорректное значение balance={balance}, "
                    f"min_price_override не будет изменён."
                )
                conn.close()
                return

            denom = 1000 - new_total
            if denom <= 0:
                print(
                    f"[lis_purchase][{account.name}] denom <= 0 при вычислении min_price, "
                    f"min_price_override не будет изменён."
                )
                conn.close()
                return

            min_price = balance_val / denom
            cur.execute(
                "UPDATE account_stats SET min_price_override = ? WHERE id = 1",
                (min_price,),
            )
            conn.commit()
            print(
                f"[lis_purchase][{account.name}] Установлен min_price_override={min_price:.6f}"
            )
        else:
            print(
                f"[lis_purchase][{account.name}] Ошибочный ответ {resp.status_code} "
                f"при запросе баланса, min_price_override не ограничиваем."
            )

    conn.close()


# ---------- обновление счётчиков в steam_analyser.db ----------


def _update_steam_item_counters(
    item_name: str,
    quant_lots: int,
    sum_lots: float,
) -> None:
    """
    Обновляем purchased_lots / purchased_sum / time_lots / time_sum
    в основной базе steam_analyser.db.
    """
    if quant_lots <= 0 and sum_lots <= 0:
        return

    row = get_cached_item(item_name)
    if row is None:
        print(
            f"[lis_purchase] В steam_items нет записи для {item_name!r}, "
            f"счётчики покупок не обновляем."
        )
        return

    existing_lots = float(row["purchased_lots"] or 0)
    existing_sum = float(row["purchased_sum"] or 0)
    existing_time_lots = parse_db_timestamp(row["time_lots"])
    existing_time_sum = parse_db_timestamp(row["time_sum"])

    now = datetime.now(timezone.utc)

    if existing_lots == 0:
        new_lots = float(quant_lots)
        new_time_lots = now
    else:
        new_lots = existing_lots + float(quant_lots)
        new_time_lots = existing_time_lots

    if existing_sum == 0:
        new_sum = float(sum_lots)
        new_time_sum = now
    else:
        new_sum = existing_sum + float(sum_lots)
        new_time_sum = existing_time_sum

    print(
        f"[lis_purchase] Обновляем steam_items для {item_name!r}: "
        f"purchased_lots={existing_lots}->{new_lots}, "
        f"purchased_sum={existing_sum}->{new_sum}"
    )
    update_purchase_tracking(
        item_name=item_name,
        purchased_lots=new_lots,
        purchased_sum=new_sum,
        time_lots=new_time_lots,
        time_sum=new_time_sum,
    )


# ---------- разбор ответов LIS (77–78) ----------

SUCCESS_STATUSES = {"wait_unlock", "wait_accept", "accepted", "processing", "wait_withdraw"}


def _extract_successful_lots_from_buy_response(
    payload: Dict[str, Any],
    custom_id: str,
) -> Tuple[List[PurchasedLot], List[int]]:
    """
    Применяет правила 77–78 к ответу /v1/market/buy.
    Возвращает:
      - список успешно купленных лотов
      - список id лотов, которые явно НЕ куплены
    """
    data = payload.get("data") or {}
    skins = data.get("skins") or []

    purchased: List[PurchasedLot] = []
    failed_ids: List[int] = []

    for skin in skins:
        lot_id = skin.get("id")
        price = skin.get("price")
        status = skin.get("status")
        error_val = skin.get("error")
        return_reason = skin.get("return_reason")

        if error_val not in (None, 0, "0"):
            print(
                f"[lis_purchase] Лот id={lot_id}: error={error_val!r}, "
                f"лот будет пропущен."
            )
            if lot_id is not None:
                failed_ids.append(int(lot_id))
            continue

        if status == "return":
            print(
                f"[lis_purchase] Лот id={lot_id}: status=return, "
                f"return_reason={return_reason!r}, лот будет пропущен."
            )
            if lot_id is not None:
                failed_ids.append(int(lot_id))
            continue

        if status not in SUCCESS_STATUSES:
            print(
                f"[lis_purchase] Лот id={lot_id}: неизвестный статус {status!r}, "
                f"лот будет пропущен."
            )
            if lot_id is not None:
                failed_ids.append(int(lot_id))
            continue

        try:
            lot_id_int = int(lot_id)
        except (TypeError, ValueError):
            print(
                f"[lis_purchase] Некорректный id лота={lot_id!r}, "
                f"лот будет пропущен."
            )
            continue

        try:
            price_val = float(price)
        except (TypeError, ValueError):
            print(
                f"[lis_purchase] Некорректная цена лота id={lot_id_int}, "
                f"лот будет пропущен."
            )
            failed_ids.append(lot_id_int)
            continue

        purchased.append(
            PurchasedLot(
                lot_id=lot_id_int,
                price=price_val,
                status=status or "",
                custom_id=custom_id,
                return_reason=return_reason,
            )
        )

    return purchased, failed_ids


def _extract_successful_lots_from_info_response(
    payload: Dict[str, Any],
    custom_id: str,
) -> Tuple[List[PurchasedLot], List[int]]:
    """То же самое, но для /v1/market/info (список покупок)."""
    entries = payload.get("data") or []
    purchased: List[PurchasedLot] = []
    failed_ids: List[int] = []

    for entry in entries:
        skins = entry.get("skins") or []
        sub_payload = {"data": {"skins": skins}}
        p, f = _extract_successful_lots_from_buy_response(sub_payload, custom_id)
        purchased.extend(p)
        failed_ids.extend(f)

    return purchased, failed_ids


# ---------- /v1/market/check-availability (66) ----------


def _check_availability(
    account: LissAccount,
    ids: List[int],
) -> Tuple[List[int], Dict[int, float]]:
    """
    Пункт 66: проверка доступности и актуальных цен.
    Возвращает:
      - список доступных id
      - словарь {id: price}
    """
    if not ids:
        return [], {}

    available_ids: List[int] = []
    id_to_price: Dict[int, float] = {}
    for chunk in _chunk_list(ids):
        params: Dict[str, Any] = {"ids[]": chunk}
        print(
            f"[lis_purchase][{account.name}] "
            f"Запрос /market/check-availability для {len(chunk)} лотов..."
        )
        try:
            resp = send_lis_request(
                "GET",
                CHECK_AVAIL_URL,
                api_key=account.api_key,
                params=params,
            )
        except requests.Timeout:
            print(
                f"[lis_purchase][{account.name}] Таймаут /market/check-availability, "
                f"лоты будут пропущены."
            )
            return [], {}

        if resp.status_code not in (200, 201):
            print(
                f"[lis_purchase][{account.name}] Ошибка {resp.status_code} "
                f"при /market/check-availability, лоты будут пропущены."
            )
            return [], {}

        try:
            payload = resp.json()
        except json.JSONDecodeError:
            print(
                f"[lis_purchase][{account.name}] Ошибка парсинга JSON для /check-availability."
            )
            return [], {}

        data = payload.get("data") or {}
        available_skins = data.get("available_skins") or {}

        for key, value in available_skins.items():
            try:
                lot_id = int(key)
                price = float(value)
            except (TypeError, ValueError):
                continue
            available_ids.append(lot_id)
            id_to_price[lot_id] = price

        unavailable = data.get("unavailable_skin_ids") or []
        if unavailable:
            print(
                f"[lis_purchase][{account.name}] Недоступные id после check-availability: "
                f"{unavailable}"
            )

        print(
            f"[lis_purchase][{account.name}] Доступно {len(available_skins)} лотов "
            f"из {len(chunk)} по /check-availability."
        )

    return available_ids, id_to_price


# ---------- /v1/market/info при таймауте покупки (54) ----------


def _handle_buy_timeout(
    account: LissAccount,
    custom_id: str,
) -> Tuple[List[PurchasedLot], List[int]]:
    """
    Пункт 54: если /market/buy упал по таймауту,
    проверяем покупку по /market/info с custom_id.
    Возвращает (purchased_lots, failed_ids).
    """
    params = {"custom_ids[]": [custom_id]}
    print(
        f"[lis_purchase][{account.name}] Таймаут /market/buy, "
        f"проверяем /market/info по custom_id={custom_id}..."
    )

    while True:
        try:
            resp = send_lis_request(
                "GET",
                INFO_URL,
                api_key=account.api_key,
                params=params,
            )
        except requests.Timeout:
            timeout_sec = getattr(config, "HTTP_TIMEOUT", 20.0)
            print(
                f"[lis_purchase][{account.name}] Таймаут /market/info, "
                f"возможно нет соединения с интернетом. "
                f"Пробуем ещё раз через {timeout_sec} секунд..."
            )
            time.sleep(timeout_sec)
            continue

        break

    if resp.status_code in (200, 201):
        try:
            payload = resp.json()
        except json.JSONDecodeError:
            print(
                f"[lis_purchase][{account.name}] Ошибка парсинга JSON /market/info."
            )
            return [], []

        purchased, failed_ids = _extract_successful_lots_from_info_response(
            payload, custom_id
        )
        if purchased:
            print(
                f"[lis_purchase][{account.name}] По /market/info найдены "
                f"{len(purchased)} купленных лотов по custom_id={custom_id}."
            )
        else:
            print(
                f"[lis_purchase][{account.name}] По /market/info не найдено купленных лотов "
                f"по custom_id={custom_id}."
            )
    return purchased, failed_ids


def _buy_batch(
    account: LissAccount,
    item_name: str,
    ids_to_buy: List[int],
    search_index: Dict[int, Dict[str, Any]],
    *,
    min_price_override: float,
) -> Tuple[int, bool]:
    """Покупает один чанк id (<= MAX_IDS_PER_REQUEST). Возвращает (purchased, stop_flag)."""
    error_counters = {
        "skins_price_higher_than_max_price": 0,
        "invalid_ids_value": 0,
        "invalid_max_price_value": 0,
        "invalid_custom_id_value": 0,
    }

    id_to_price = {i: float(search_index[i]["price"]) for i in ids_to_buy}

    while ids_to_buy:
        custom_id = _random_custom_id()
        print(
            f"[lis_purchase][{account.name}] Попытка покупки {len(ids_to_buy)} лотов, "
            f"custom_id={custom_id}"
        )

        sum_price = sum(id_to_price[i] for i in ids_to_buy)
        max_price = sum_price + 0.02

        body = {
            "ids": ids_to_buy,
            "partner": account.partner,
            "token": account.token,
            "max_price": max_price,
            "custom_id": custom_id,
            "skip_unavailable": True,
        }

        print(
            f"[lis_purchase][{account.name}] POST /market/buy, "
            f"max_price={max_price:.6f}, ids={ids_to_buy}"
        )

        try:
            resp = send_lis_request(
                "POST",
                BUY_URL,
                api_key=account.api_key,
                json_data=body,
                timeout=getattr(config, "HTTP_TIMEOUT", 20.0),
            )
        except requests.Timeout:
            print(
                f"[lis_purchase][{account.name}] Таймаут /market/buy, "
                f"переходим к проверке /market/info..."
            )
            purchased, _failed_ids = _handle_buy_timeout(account, custom_id)
            if purchased:
                total, short_count, sum_price = _save_purchased_lots_to_db(
                    account, item_name, purchased, search_index
                )
                _update_steam_item_counters(item_name, total, sum_price)
                _update_short_hold_stats_and_maybe_min_price(account, short_count)
                return total, False

            print(
                f"[lis_purchase][{account.name}] По /market/info покупка не подтвердилась, "
                f"лоты будут пропущены."
            )
            return 0, True

        print(
            f"[lis_purchase][{account.name}] Ответ /market/buy: HTTP {resp.status_code}"
        )

        # 51) Успешный ответ
        if resp.status_code in (200, 201):
            try:
                payload = resp.json()
            except json.JSONDecodeError:
                print(
                    f"[lis_purchase][{account.name}] Ошибка парсинга JSON в ответе /market/buy."
                )
                return 0, True

            purchased, failed_ids = _extract_successful_lots_from_buy_response(
                payload, custom_id
            )
            total, short_count, sum_price = _save_purchased_lots_to_db(
                account, item_name, purchased, search_index
            )
            _update_steam_item_counters(item_name, total, sum_price)
            _update_short_hold_stats_and_maybe_min_price(account, short_count)

            if failed_ids:
                print(
                    f"[lis_purchase][{account.name}] "
                    f"{len(failed_ids)} лотов были отклонены логикой 77–78 и пропущены."
                )
            return total, False

        # 52) HTTP 400
        if resp.status_code == 400:
            try:
                payload = resp.json()
            except json.JSONDecodeError:
                print(
                    f"[lis_purchase][{account.name}] HTTP 400 без корректного JSON, "
                    f"лоты будут пропущены."
                )
                return 0, True

            error_val = (payload.get("error") or "").strip()
            print(f"[lis_purchase][{account.name}] HTTP 400 error={error_val!r}")

            if error_val == "custom_id_already_exists":
                print(
                    f"[lis_purchase][{account.name}] custom_id_already_exists, "
                    f"пробуем с новым custom_id..."
                )
                continue

            if error_val == "skins_unavailable":
                names = [search_index[i].get("name") for i in ids_to_buy if i in search_index]
                print(
                    f"[lis_purchase][{account.name}] Лоты недоступны (skins_unavailable): {names}"
                )
                return 0, True

            if error_val == "skins_price_higher_than_max_price":
                error_counters["skins_price_higher_than_max_price"] += 1
                if error_counters["skins_price_higher_than_max_price"] >= 2:
                    print(
                        f"[lis_purchase][{account.name}] "
                        f"skins_price_higher_than_max_price второй раз подряд, "
                        f"лоты будут пропущены."
                    )
                    return 0, True

                available_ids, id_price_map = _check_availability(account, ids_to_buy)
                if not available_ids:
                    print(
                        f"[lis_purchase][{account.name}] После check-availability "
                        f"нет доступных лотов, прекращаем."
                    )
                    return 0, True
                ids_to_buy = available_ids
                id_to_price = id_price_map
                continue

            if error_val in {
                "invalid_trade_url",
                "user_trade_ban",
                "user_cant_trade",
                "private_inventory",
                "too_many_failed_attempts_for_user",
            }:
                print(
                    f"[lis_purchase][{account.name}] Ошибка аккаунта {error_val!r}, "
                    f"переходим к следующему аккаунту."
                )
                raise SkipAccountError(error_val)

            print(
                f"[lis_purchase][{account.name}] Неизвестная ошибка 400 {error_val!r}, "
                f"лоты будут пропущены."
            )
            return 0, True

        # 53) HTTP 422
        if resp.status_code == 422:
            try:
                payload = resp.json()
            except json.JSONDecodeError:
                print(
                    f"[lis_purchase][{account.name}] HTTP 422 без корректного JSON, "
                    f"лоты будут пропущены."
                )
                return 0, True

            error_val = (payload.get("error") or "").strip()
            print(f"[lis_purchase][{account.name}] HTTP 422 error={error_val!r}")

            if error_val in {"invalid_partner_value", "invalid_token_value"}:
                print(
                    f"[lis_purchase][{account.name}] Ошибка аккаунта {error_val!r}, "
                    f"переходим к следующему аккаунту."
                )
                raise SkipAccountError(error_val)

            if error_val in {"invalid_ids_value", "invalid_max_price_value"}:
                error_counters[error_val] += 1
                if error_counters[error_val] >= 2:
                    print(
                        f"[lis_purchase][{account.name}] {error_val} второй раз подряд, "
                        f"лоты будут пропущены."
                    )
                    return 0, True

                available_ids, id_price_map = _check_availability(account, ids_to_buy)
                if not available_ids:
                    print(
                        f"[lis_purchase][{account.name}] После check-availability "
                        f"нет доступных лотов, прекращаем."
                    )
                    return 0, True
                ids_to_buy = available_ids
                id_to_price = id_price_map
                continue

            if error_val == "invalid_custom_id_value":
                error_counters["invalid_custom_id_value"] += 1
                if error_counters["invalid_custom_id_value"] >= 2:
                    print(
                        f"[lis_purchase][{account.name}] invalid_custom_id_value второй раз подряд, "
                        f"лоты будут пропущены."
                    )
                    return 0, True
                print(
                    f"[lis_purchase][{account.name}] invalid_custom_id_value, "
                    f"пробуем с новым custom_id..."
                )
                continue

            if error_val == "after_ids":
                min_price_override = max(min_price_override, 0.01)
                ids_to_buy = [
                    lot_id
                    for lot_id in ids_to_buy
                    if float(search_index[lot_id].get("price", 0)) >= min_price_override
                ]
                id_to_price = {i: float(search_index[i]["price"]) for i in ids_to_buy}
                print(
                    f"[lis_purchase][{account.name}] after_ids, "
                    f"повторяем покупку с min_price_override={min_price_override:.6f}"
                )
                continue

            print(
                f"[lis_purchase][{account.name}] Неизвестная ошибка 422 {error_val!r}, "
                f"лоты будут пропущены."
            )
            return 0, True

        print(
            f"[lis_purchase][{account.name}] Неожиданный HTTP-код {resp.status_code}, "
            f"лоты будут пропущены."
        )
        return 0, True

    return 0, False

    print(
        f"[lis_purchase][{account.name}] Ошибка {resp.status_code} по /market/info "
        f"для custom_id={custom_id}, покупка считается неуспешной."
    )
    return [], []


# ---------- сохранение купленных лотов в БД (79) ----------


def _save_purchased_lots_to_db(
    account: LissAccount,
    item_name: str,
    purchased_lots: List[PurchasedLot],
    search_index: Dict[int, Dict[str, Any]],
) -> Tuple[int, int, float]:
    """
    Сохраняем купленные лоты в БД {account.name}_liss_purchase.db.
    Возвращает:
      - count_total: сколько лотов сохранено
      - short_hold_count: сколько из них с холдом 0–7
      - sum_price: суммарная стоимость
    """
    if not purchased_lots:
        return 0, 0, 0.0

    conn = _get_purchase_db_conn(account.name)
    cur = conn.cursor()

    short_hold_count = 0
    sum_price = 0.0
    count_total = 0

    for pl in purchased_lots:
        lot_info = search_index.get(pl.lot_id, {})
        item_name_row = lot_info.get("name") or item_name
        unlock_at = lot_info.get("unlock_at")
        created_at = lot_info.get("created_at")
        item_float = lot_info.get("item_float")
        hold_days = lot_info.get("hold_days", 0)
        stickers = lot_info.get("stickers") or []

        sticker_names: List[Optional[str]] = []
        sticker_wears: List[Optional[float]] = []
        for st in stickers[:5]:
            sticker_names.append(st.get("name"))
            sticker_wears.append(st.get("wear"))
        while len(sticker_names) < 5:
            sticker_names.append(None)
            sticker_wears.append(None)

        cur.execute(
            """
            INSERT INTO purchases (
                lot_id, item_name, price,
                unlock_at, created_at, item_float, hold_days,
                custom_id, status, return_reason,
                sticker1, wear1,
                sticker2, wear2,
                sticker3, wear3,
                sticker4, wear4,
                sticker5, wear5
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                pl.lot_id,
                item_name_row,
                pl.price,
                unlock_at,
                created_at,
                item_float,
                hold_days,
                pl.custom_id,
                pl.status,
                pl.return_reason,
                sticker_names[0], sticker_wears[0],
                sticker_names[1], sticker_wears[1],
                sticker_names[2], sticker_wears[2],
                sticker_names[3], sticker_wears[3],
                sticker_names[4], sticker_wears[4],
            ),
        )

        sum_price += pl.price
        count_total += 1
        if 0 <= int(hold_days) <= 7:
            short_hold_count += 1

    conn.commit()
    conn.close()

    print(
        f"[lis_purchase][{account.name}] В purchases добавлено {count_total} лотов, "
        f"сумма={sum_price:.6f}, с холдом 0–7 = {short_hold_count}"
    )

    return count_total, short_hold_count, sum_price


# ---------- покупка на одном аккаунте (51–53, 66) ----------


def _random_custom_id() -> str:
    """Случайное 7-значное число в строке."""
    return f"{random.randint(1000000, 9999999)}"


def _buy_with_account(
    account: LissAccount,
    item_name: str,
    lots_from_search: List[Dict[str, Any]],
) -> int:
    """
    Пытаемся купить переданные лоты на одном аккаунте.
    Возвращает количество успешно купленных лотов с холдом 0–7.
    """
    if not lots_from_search:
        print(f"[lis_purchase][{account.name}] Нет лотов для покупки.")
        return 0

    min_price_override = _get_min_price_override(account.name)
    if min_price_override > 0:
        lots = [
            l for l in lots_from_search
            if float(l.get("price", 0)) >= min_price_override
        ]
        print(
            f"[lis_purchase][{account.name}] Применён min_price_override={min_price_override:.6f}, "
            f"осталось {len(lots)} лотов из {len(lots_from_search)}."
        )
    else:
        lots = list(lots_from_search)

    if not lots:
        print(f"[lis_purchase][{account.name}] После min_price_override лотов не осталось.")
        return 0

    search_index: Dict[int, Dict[str, Any]] = {}
    ids: List[int] = []
    for l in lots:
        try:
            lot_id = int(l["id"])
        except (KeyError, TypeError, ValueError):
            continue
        ids.append(lot_id)
        search_index[lot_id] = l

    if not ids:
        print(f"[lis_purchase][{account.name}] Не смогли извлечь id лотов.")
        return 0

    total_purchased = 0
    for batch in _chunk_list(ids, MAX_IDS_PER_REQUEST):
        purchased, stop = _buy_batch(
            account,
            item_name,
            batch,
            search_index,
            min_price_override=min_price_override,
        )
        total_purchased += purchased
        if stop:
            break

    return total_purchased

# ---------- Публичная функция для использования из lissbot.py ----------


def purchase_lis_skins_for_item(
    item_name: str,
    lots_from_search: List[Dict[str, Any]],
    accounts: List[LissAccount],
) -> int:
    """
    Главная функция для внешнего использования.
    Принимает название предмета и список лотов (из lis_search.search_lis_skins),
    пытается купить их на одном из аккаунтов.
    Возвращает количество УСПЕШНО купленных лотов с холдом 0–7.
    """
    print("=" * 80)
    print(f"[lis_purchase] Старт покупки для предмета: {item_name}")

    if not lots_from_search:
        print("[lis_purchase] Список лотов пуст, покупать нечего.")
        return 0

    if not accounts:
        print("[lis_purchase] Список LIS-аккаунтов пуст, покупка невозможна.")
        return 0

    for acc in accounts:
        print(f"[lis_purchase] Пробуем аккаунт: {acc.name}")
        try:
            purchased_count = _buy_with_account(acc, item_name, lots_from_search)
        except SkipAccountError as e:
            print(
                f"[lis_purchase] Аккаунт {acc.name} пропущен из-за ошибки: {e}"
            )
            continue

        if purchased_count > 0:
            print(
                f"[lis_purchase] На аккаунте {acc.name} куплено "
                f"{purchased_count} лотов."
            )
            return purchased_count

    print(
        "[lis_purchase] Попытки покупки на всех аккаунтах завершены, "
        "подходящих покупок не произведено."
    )
    return 0

