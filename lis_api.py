# lis_api.py
"""
Работа с LIS Public API (market/search) + фильтрация лотов.

Основная точка входа:
    process_item(item_name, quantity_max_allowed, sum_max_allowed)
"""

import json
import math
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

import config
import priceAnalys


MARKET_SEARCH_URL = "https://api.lis-skins.com/v1/market/search"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FILTR_PARS_DIR = os.path.join(BASE_DIR, "filtr_pars")
os.makedirs(FILTR_PARS_DIR, exist_ok=True)


@dataclass
class LisAccount:
    name: str
    api_key: str
    partner: str
    token: str


def _load_liss_accounts(path: Optional[str] = None) -> List[LisAccount]:
    """
    Читает liss_accs.txt в формате:
        name_acc@api_key:partner:token
    Возвращает список LisAccount.
    """
    if path is None:
        path = os.path.join(BASE_DIR, "liss_accs.txt")

    if not os.path.exists(path):
        raise FileNotFoundError(f"Файл с аккаунтами LIS не найден: {path}")

    accounts: List[LisAccount] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                left, partner, token = line.split(":", 2)
                name, api_key = left.split("@", 1)
            except ValueError:
                raise ValueError(
                    f"Некорректный формат строки в {path!r}: {line!r} "
                    "(ожидалось name@api_key:partner:token)"
                )
            accounts.append(LisAccount(name=name, api_key=api_key, partner=partner, token=token))

    if not accounts:
        raise RuntimeError(f"В файле {path} нет ни одного аккаунта LIS.")
    return accounts


def _parse_unlock_at_to_hold_days(unlock_at: Optional[str]) -> int:
    """
    Переводит unlock_at (строка вида '2024-07-22T11:31:01.000000Z')
    в количество дней холда, считая от текущего UTC, округление всегда ВВЕРХ.
    Если unlock_at == None или парсинг не удался → 0 дней.
    """
    if not unlock_at:
        return 0

    try:
        value = unlock_at.strip()
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        delta_sec = (dt - now).total_seconds()
        if delta_sec <= 0:
            return 0
        days = delta_sec / 86400.0
        return int(math.ceil(days))
    except Exception:
        return 0


def _compute_max_price_by_profit(item_name: str) -> float:
    """
    Берёт rec_price из БД и считает максимальную цену покупки по формуле:
        max_price = (rec_price * 0.8697) / (LISS_MIN_PROFIT + 1)
    """
    row = priceAnalys.get_cached_item(item_name)
    if row is None:
        raise RuntimeError(
            f"В БД нет записей по предмету '{item_name}'. "
            "Сначала нужно прогнать анализ Steam (main.py или lissbot.py)."
        )

    try:
        rec_price = float(row["rec_price"] or 0.0)
    except Exception:
        rec_price = 0.0

    if rec_price <= 0:
        raise RuntimeError(
            f"Для предмета '{item_name}' в БД rec_price <= 0. "
            "Нельзя посчитать максимальную цену покупки."
        )

    adjusted_rec_price = rec_price * 0.8697
    max_price = adjusted_rec_price / (config.LISS_MIN_PROFIT + 1.0)
    return max_price


def _fetch_lis_items(
    item_name: str,
    api_key: str,
    max_price_by_profit: float,
) -> List[Dict[str, Any]]:
    """
    Запрашивает все лоты по предмету через /v1/market/search (все страницы по cursor).
    Возвращает список элементов из поля "data".
    """
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

    unlock_days = list(range(0, config.LISS_MAX_HOLD_DAYS + 1))

    params: Dict[str, Any] = {
        "game": "csgo",
        "names[]": item_name,
        "price_from": config.LISS_EXTRA_MIN_PRICE,
        # ВАЖНО: теперь price_to = (rec_price * 0.8697) / (LISS_MIN_PROFIT + 1)
        "price_to": max_price_by_profit,
        "sort_by": "lowest_price",
        "unlock_days[]": unlock_days,
        "per_page": 200,
    }

    all_items: List[Dict[str, Any]] = []
    cursor: Optional[str] = None

    while True:
        if cursor:
            params["cursor"] = cursor
        else:
            params.pop("cursor", None)

        resp = requests.get(
            MARKET_SEARCH_URL,
            headers=headers,
            params=params,
            timeout=config.HTTP_TIMEOUT,
        )

        if resp.status_code == 429:
            time.sleep(2)
            continue

        resp.raise_for_status()
        data = resp.json()

        page_items = data.get("data") or []
        if not isinstance(page_items, list):
            raise ValueError("Ожидается, что поле 'data' — список.")

        all_items.extend(page_items)

        meta = data.get("meta") or {}
        cursor = meta.get("next_cursor")
        if not cursor:
            break

    return all_items


def _save_stage_file(stage: int, item_name: str, payload: Any) -> str:
    """
    Сохраняет данные в filtr_pars/ Nfiltr_<safe_name>.txt
    """
    safe_name = priceAnalys.safe_filename(item_name)
    filename = f"{stage}filtr_{safe_name}.txt"
    path = os.path.join(FILTR_PARS_DIR, filename)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[LIS_API][WARN] Не удалось сохранить файл {path}: {e}")
    return path


def process_item(
    item_name: str,
    quantity_max_allowed: int,
    sum_max_allowed: float,
    *,
    account_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Главная функция:
      1) берёт rec_price из БД и считает max_price по прибыли;
      2) скачивает лоты с LIS;
      3) фильтрует по цене;
      4) считает холд (в днях);
      5) при необходимости режет список по QUANTITY_MAX_ALLOWED и SUM_MAX_ALLOWED.

    Возвращает словарь с кратким резюме и финальным списком лотов.
    Также создаёт файлы:
      filtr_pars/0filtr_<name>.txt — сырой ответ (основные поля)
      filtr_pars/1filtr_<name>.txt — после фильтрации по прибыли
      filtr_pars/2filtr_<name>.txt — после ограничения по количеству/сумме
    """
    if quantity_max_allowed <= 0:
        raise ValueError("QUANTITY_MAX_ALLOWED должен быть > 0")
    if sum_max_allowed <= 0:
        raise ValueError("SUM_MAX_ALLOWED должен быть > 0")

    accounts = _load_liss_accounts()
    if account_name:
        account = next((a for a in accounts if a.name == account_name), None)
        if account is None:
            raise RuntimeError(
                f"Аккаунт с именем '{account_name}' не найден в liss_accs.txt"
            )
    else:
        account = accounts[0]

    # считаем максимальную цену по формуле из rec_price
    max_price_by_profit = _compute_max_price_by_profit(item_name)

    # 1) запрос к LIS (price_to уже = max_price_by_profit)
    raw_items = _fetch_lis_items(item_name, account.api_key, max_price_by_profit)

    # сохраняем stage0 (сырая выдача)
    stage0_payload = {
        "item_name": item_name,
        "account": account.name,
        "params": {
            "price_from": config.LISS_EXTRA_MIN_PRICE,
            "price_to": max_price_by_profit,
            "max_price_by_profit": max_price_by_profit,
            "max_hold_days": config.LISS_MAX_HOLD_DAYS,
        },
        "items": raw_items,
    }
    file0 = _save_stage_file(0, item_name, stage0_payload)

    # 2) фильтр по цене (<= max_price_by_profit) + расчёт hold_days
    filtered_items: List[Dict[str, Any]] = []
    for it in raw_items:
        try:
            price = float(it.get("price", 0))
        except (TypeError, ValueError):
            continue

        if price > max_price_by_profit:
            continue

        unlock_at = it.get("unlock_at")
        hold_days = _parse_unlock_at_to_hold_days(unlock_at)

        filtered_items.append(
            {
                "id": it.get("id"),
                "price": price,
                "unlock_at": unlock_at,
                "hold_days": hold_days,
            }
        )

    total_filtered_quantity = len(filtered_items)
    total_filtered_sum = sum(it["price"] for it in filtered_items)

    stage1_payload = {
        "item_name": item_name,
        "account": account.name,
        "max_price_by_profit": max_price_by_profit,
        "items": filtered_items,
        "total_quantity": total_filtered_quantity,
        "total_sum": total_filtered_sum,
    }
    file1 = _save_stage_file(1, item_name, stage1_payload)

    # 3) проверка лимитов по кол-ву и сумме
    if (
        total_filtered_quantity <= quantity_max_allowed
        and total_filtered_sum <= sum_max_allowed
    ):
        final_items = filtered_items
    else:
        # сортировка по цене (asc), затем по холду (asc)
        sorted_items = sorted(
            filtered_items,
            key=lambda x: (x["price"], x["hold_days"]),
        )

        final_items: List[Dict[str, Any]] = []
        running_sum = 0.0
        running_count = 0

        for it in sorted_items:
            next_count = running_count + 1
            next_sum = running_sum + it["price"]

            if next_count > quantity_max_allowed or next_sum > sum_max_allowed:
                break

            final_items.append(it)
            running_count = next_count
            running_sum = next_sum

    final_quantity = len(final_items)
    final_sum = sum(it["price"] for it in final_items)

    stage2_payload = {
        "item_name": item_name,
        "account": account.name,
        "quantity_max_allowed": quantity_max_allowed,
        "sum_max_allowed": sum_max_allowed,
        "max_price_by_profit": max_price_by_profit,
        "total_after_price_filter": {
            "quantity": total_filtered_quantity,
            "sum": total_filtered_sum,
        },
        "final": {
            "quantity": final_quantity,
            "sum": final_sum,
            "items": final_items,
        },
    }
    file2 = _save_stage_file(2, item_name, stage2_payload)

    return {
        "item_name": item_name,
        "account": account.name,
        "max_price_by_profit": max_price_by_profit,
        "raw_count": len(raw_items),
        "total_filtered_quantity": total_filtered_quantity,
        "total_filtered_sum": total_filtered_sum,
        "final_quantity": final_quantity,
        "final_sum": final_sum,
        "items": final_items,
        "files": {
            "stage0": file0,
            "stage1": file1,
            "stage2": file2,
        },
    }
