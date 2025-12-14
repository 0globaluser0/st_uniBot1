"""Утилита для предварительного анализа лотов lis-skins.

Скрипт скачивает список предметов, применяет набор фильтров и выводит
сообщения о готовности лотов к дальнейшему анализу + авто-покупка на LIS.

ВАЖНО (новая логика):
- Лоты (id/price/unlock_at/...) берём НЕ через API market/search,
  а из локального SQLite-индекса, построенного из полного дампа
  https://lis-skins.com/market_export_json/api_csgo_full.json
- Полный дамп качается/индексируется в фоне каждые PRICE_REFRESH_MINUTES,
  цикл НЕ останавливаем, но в начале каждой итерации дожидаемся завершения
  фонового обновления, чтобы использовать свежий индекс.
"""

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote

import inspect
import lis_search

import requests

import config
import priceAnalys

from lis_full_dump import FullDumpUpdater
from lis_search import search_lis_skins
from lis_purchase import (
    LissAccount,
    load_liss_accounts,
    init_purchase_dbs,
    purchase_lis_skins_for_item,
)

MARKET_URL = "https://lis-skins.com/market_export_json/csgo.json"
STEAM_BASE_URL = "https://steamcommunity.com/market/listings/730/"

LISS_ACCOUNTS: List[LissAccount] = []

# Активный путь к SQLite индексу полного дампа (A/B)
FULL_DUMP: Optional[FullDumpUpdater] = None
ACTIVE_FULL_DB: Optional[Path] = None


def auto_search_and_buy(
    *,
    item_name: str,
    rec_price: float,
    quantity_max_allowed: float,
    sum_max_allowed: float,
) -> int:
    """
    Автоматический поиск лотов на LIS-SKINS и покупка.
    Возвращает ОБЩЕЕ количество купленных лотов по этому предмету (холд не важен).
    0 = ничего не купили или произошла ошибка.
    """
    if not LISS_ACCOUNTS:
        print("[LISS][WARN] Нет LIS-аккаунтов, покупка пропущена.")
        return 0

    if ACTIVE_FULL_DB is None or not ACTIVE_FULL_DB.exists():
        print("[LISS][WARN] Нет активного индекса полного дампа (ACTIVE_FULL_DB), покупка пропущена.")
        return 0

    # 1) Поиск лотов в локальном индексе
    try:
        lots = search_lis_skins(
            item_name=item_name,
            rec_price=rec_price,
            quantity_max_allowed=quantity_max_allowed,
            sum_max_allowed=sum_max_allowed,
            db_path=ACTIVE_FULL_DB,
        )
    except Exception as exc:
        print(f"[LISS][ERROR] Ошибка поиска лотов для {item_name!r}: {exc}")
        return 0

    if not lots:
        print(
            f"[LISS] Поиск лотов на LIS-SKINS для {item_name!r} "
            f"вернул 0 результатов, покупка пропущена."
        )
        return 0

    # 2) Покупка (purchase_lis_skins_for_item возвращает ОБЩЕЕ кол-во купленных лотов)
    try:
        purchased_count = purchase_lis_skins_for_item(
            item_name=item_name,
            lots_from_search=lots,
            accounts=LISS_ACCOUNTS,
        )
    except Exception as exc:
        print(f"[LISS][ERROR] Ошибка покупки лотов для {item_name!r}: {exc}")
        return 0

    if purchased_count is None:
        return 0

    try:
        return int(purchased_count)
    except (TypeError, ValueError):
        return 0


def build_steam_url(name: str) -> str:
    """Формирует ссылку на страницу предмета в Steam."""
    return f"{STEAM_BASE_URL}{quote(name)}"


def is_blacklisted(name: str) -> bool:
    """True, если предмет в актуальном блэклисте."""
    entry_info = priceAnalys.get_active_blacklist_entry(name)
    return entry_info is not None


def fetch_market_items() -> List[Dict[str, object]]:
    response = requests.get(MARKET_URL, timeout=config.HTTP_TIMEOUT)
    response.raise_for_status()
    data = response.json()

    if not isinstance(data, list):
        raise ValueError("Ожидался список предметов из lis-skins")

    items: List[Dict[str, object]] = []
    for raw in data:
        if not isinstance(raw, dict):
            continue
        name = str(raw.get("name", "")).strip()
        price = raw.get("price")
        try:
            price_value = float(price)
        except (TypeError, ValueError):
            continue
        items.append({"name": name, "price": price_value})
    return items


def filter_by_keywords_and_price(items: Iterable[Dict[str, object]]) -> List[Dict[str, object]]:
    filtered: List[Dict[str, object]] = []
    total = 0
    for item in items:
        total += 1
        name = str(item.get("name", ""))
        price = float(item.get("price", 0))
        lower_name = name.lower()

        if any(keyword.lower() in lower_name for keyword in config.LISS_BLACKLIST_KEYWORDS):
            continue
        if is_blacklisted(name):
            continue
        if price > config.LISS_MAX_PRICE:
            continue
        if price < config.LISS_EXTRA_MIN_PRICE:
            continue

        filtered.append({"name": name, "price": price})

    print(f"[LISS] отсортировано {len(filtered)} из {total} предметов после первичных фильтров")
    return filtered


def load_known_items() -> List[Dict[str, object]]:
    """Загружает предметы с рек. ценой и свежими данными."""
    conn = priceAnalys.get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT item_name, rec_price, avg_sales, purchased_lots, purchased_sum,
               time_lots, time_sum, updated_at
        FROM steam_items
        WHERE rec_price IS NOT NULL
        """
    )
    rows = cur.fetchall()
    conn.close()

    items: List[Dict[str, object]] = []
    now = datetime.utcnow()
    for row in rows:
        updated_at = row["updated_at"]
        if updated_at and config.ACTUAL_HOURS > 0:
            try:
                dt_updated = datetime.fromisoformat(updated_at)
            except Exception:
                dt_updated = None
            if dt_updated is not None:
                age_hours = (now - dt_updated).total_seconds() / 3600.0
                if age_hours > config.ACTUAL_HOURS:
                    continue

        items.append({
            "name": row["item_name"],
            "rec_price": float(row["rec_price"]),
            "avg_sales": float(row["avg_sales"] or 0),
            "purchased_lots": float(row["purchased_lots"] or 0),
            "purchased_sum": float(row["purchased_sum"] or 0),
            "time_lots": row["time_lots"],
            "time_sum": row["time_sum"],
        })
    return items


def evaluate_purchase_limits(
    name: str,
    avg_sales: float,
    purchased_lots: float,
    purchased_sum: float,
    time_lots: Optional[object],
    time_sum: Optional[object],
    *,
    has_db_entry: bool,
    proxy_tag: Optional[str] = None,
) -> Optional[Tuple[float, float, float, float, Optional[datetime], Optional[datetime]]]:
    now = datetime.utcnow()
    base_allowed_lots = (config.LISS_QUANTITY_PERCENT * avg_sales) / 100.0
    quantity_max_allowed = round(base_allowed_lots)
    sum_max_allowed = config.LISS_SUM_LIMIT

    normalized_time_lots = priceAnalys.parse_db_timestamp(time_lots)
    normalized_time_sum = priceAnalys.parse_db_timestamp(time_sum)

    # Если в БД записано время с tzinfo (+00:00), приводим к naive, чтобы не было ошибки вычитания
    if normalized_time_lots is not None and getattr(normalized_time_lots, "tzinfo", None) is not None:
        normalized_time_lots = normalized_time_lots.replace(tzinfo=None)
    if normalized_time_sum is not None and getattr(normalized_time_sum, "tzinfo", None) is not None:
        normalized_time_sum = normalized_time_sum.replace(tzinfo=None)

    updated_lots = purchased_lots
    updated_sum = purchased_sum

    updated_time_lots = normalized_time_lots
    updated_time_sum = normalized_time_sum
    changed = False

    if has_db_entry:
        if normalized_time_lots and now - normalized_time_lots < timedelta(days=config.LISS_LOTS_PERIOD_DAYS):
            quantity_max_allowed = round(base_allowed_lots - purchased_lots)
            if quantity_max_allowed <= 0:
                print(f"[LISS][INFO] {name} достиг лимита по кол-ву", proxy_tag=proxy_tag)
                return None
        else:
            updated_lots = 0.0
            updated_time_lots = now
            quantity_max_allowed = round(base_allowed_lots)
            changed = True

        # --- SUM RANGE LOGIC ---
        sum_cfg = getattr(config, "LISS_SUM_LIMIT", 0)
        if isinstance(sum_cfg, (tuple, list)) and len(sum_cfg) == 2:
            sum_low, sum_high = float(sum_cfg[0]), float(sum_cfg[1])
        else:
            sum_low = sum_high = float(sum_cfg)

        if normalized_time_sum and now - normalized_time_sum < timedelta(days=config.LISS_SUM_PERIOD_DAYS):
            # если уже вошли в диапазон — стоп покупок
            if purchased_sum >= sum_low:
                print(
                    f"[LISS][INFO] {name} достиг диапазона по сумме "
                    f"(sum={purchased_sum:.2f} >= low={sum_low:.2f})",
                    proxy_tag=proxy_tag,
                )
                return None

            sum_max_allowed = sum_high - purchased_sum
            if sum_max_allowed <= 0:
                print(f"[LISS][INFO] {name} достиг лимита по сумме", proxy_tag=proxy_tag)
                return None
        else:
            updated_sum = 0.0
            updated_time_sum = now
            sum_max_allowed = sum_high
            changed = True

        if changed:
            priceAnalys.update_purchase_tracking(
                name,
                updated_lots,
                updated_sum,
                updated_time_lots,
                updated_time_sum,
            )

    return (
        quantity_max_allowed,
        sum_max_allowed,
        updated_lots,
        updated_sum,
        updated_time_lots,
        updated_time_sum,
    )


def evaluate_known_items(
    market_items: List[Dict[str, object]],
    known_items: List[Dict[str, object]],
    stop_at: Optional[float] = None,
) -> Tuple[List[str], int, int, int]:
    """
    Возвращает:
      processed_names,
      passed_filters,
      profit_passed,
      purchased_items  # кол-во предметов, по которым была хотя бы одна покупка
    """
    market_map = {item["name"]: item for item in market_items}
    candidates = [item for item in known_items if item["name"] in market_map]
    processed: List[str] = []
    passed_filters = 0
    profit_passed = 0
    purchased_items = 0

    green = "\033[92m"
    reset = "\033[0m"

    total = len(candidates)
    for idx, known in enumerate(candidates, start=1):
        priceAnalys.set_progress(idx, total)
        name = known["name"]
        market_item = market_map.get(name)

        processed.append(name)

        limits = evaluate_purchase_limits(
            name,
            known["avg_sales"],
            known["purchased_lots"],
            known["purchased_sum"],
            known.get("time_lots"),
            known.get("time_sum"),
            has_db_entry=True,
        )
        if limits is None:
            continue
        (quantity_max_allowed, sum_max_allowed, _, _, _, _) = limits

        rec_price = float(known["rec_price"])
        adjusted_rec_price = rec_price * 0.8697
        if adjusted_rec_price <= 0:
            continue

        price = float(market_item["price"])
        if price <= 0:
            continue

        profit = adjusted_rec_price / price - 1
        if profit > config.LISS_MIN_PROFIT:
            passed_filters += 1
            print(
                f"[LISS] \"{name}\": {profit:.4f} выше {config.LISS_MIN_PROFIT} - "
                f"{green}approve{reset} (qty_left={quantity_max_allowed:.2f}, sum_left={sum_max_allowed:.2f})"
            )
            print(
                "[LISS] предчек: предмет прошел фильтры и готов к парсингу id "
                f"(qty_left={quantity_max_allowed:.2f}, sum_left={sum_max_allowed:.2f})"
            )

            purchased_lots = auto_search_and_buy(
                item_name=name,
                rec_price=rec_price,
                quantity_max_allowed=quantity_max_allowed,
                sum_max_allowed=sum_max_allowed,
            )

            if (purchased_lots or 0) > 0:
                purchased_items += 1

            profit_passed += 1
        else:
            passed_filters += 1

        if stop_at is not None and time.time() >= stop_at:
            print("[LISS][TIMER] Таймер сработал во время проверки известных предметов, завершаем итерацию.")
            break

    priceAnalys.set_progress(None, None)
    return processed, passed_filters, profit_passed, purchased_items


def get_purchase_stats(name: str) -> Dict[str, object]:
    """Возвращает словарь с данными по покупкам и признаком наличия в БД."""
    conn = priceAnalys.get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT avg_sales, purchased_lots, purchased_sum, time_lots, time_sum
        FROM steam_items
        WHERE item_name = ?
        """,
        (name,),
    )
    row = cur.fetchone()
    conn.close()
    if row is None:
        return {
            "exists": False,
            "avg_sales": 0.0,
            "purchased_lots": 0.0,
            "purchased_sum": 0.0,
            "time_lots": None,
            "time_sum": None,
        }
    return {
        "exists": True,
        "avg_sales": float(row["avg_sales"] or 0),
        "purchased_lots": float(row["purchased_lots"] or 0),
        "purchased_sum": float(row["purchased_sum"] or 0),
        "time_lots": row["time_lots"],
        "time_sum": row["time_sum"],
    }


def process_new_items(
    market_items: List[Dict[str, object]], processed_names: Iterable[str], stop_at: Optional[float] = None
) -> None:
    processed_set = set(processed_names)

    targets: List[Dict[str, object]] = []
    for item in market_items:
        name = item["name"]
        price = float(item["price"])
        if name in processed_set:
            continue
        if price < config.LISS_MIN_PRICE:
            continue
        if is_blacklisted(name):
            continue

        purchase_stats = get_purchase_stats(name)
        avg_sales = float(purchase_stats.get("avg_sales", 0))
        purchased_lots = float(purchase_stats.get("purchased_lots", 0))
        purchased_sum = float(purchase_stats.get("purchased_sum", 0))
        limits = evaluate_purchase_limits(
            name,
            avg_sales,
            purchased_lots,
            purchased_sum,
            purchase_stats.get("time_lots"),
            purchase_stats.get("time_sum"),
            has_db_entry=bool(purchase_stats.get("exists")),
        )
        if limits is None:
            continue

        item_with_limits = {**item, "purchase_limits": limits, "purchase_stats": purchase_stats}
        targets.append(item_with_limits)

    total = len(targets)
    for idx, item in enumerate(targets, start=1):
        priceAnalys.set_progress(idx, total)
        name = item["name"]
        price = float(item["price"])
        purchase_limits = item.get("purchase_limits")
        purchase_stats = item.get("purchase_stats", {})
        (
            quantity_max_allowed,
            sum_max_allowed,
            purchased_lots,
            purchased_sum,
            time_lots,
            time_sum,
        ) = purchase_limits if purchase_limits else (0, 0, 0, 0, None, None)

        steam_url = build_steam_url(name)

        result = priceAnalys.parsing_steam_sales(steam_url, log_blacklist_reason=False)
        status = result.get("status")
        proxy_tag = result.get("proxy_tag")

        if status == "invalid_link":
            print(f"[LISS][WARN] {name}: некорректная ссылка {steam_url}", proxy_tag=proxy_tag)
            continue
        if status == "dota_soon":
            print(f"[LISS][INFO] {name}: анализ Dota пока не поддерживается", proxy_tag=proxy_tag)
            continue
        if status == "blacklist":
            print(f"[LISS][INFO] {name}: в блэклисте ({result.get('reason')})", proxy_tag=proxy_tag)
            continue
        if status == "error":
            print(f"[LISS][ERROR] {name}: {result.get('message')}", proxy_tag=proxy_tag)
            continue
        if status != "ok":
            print(f"[LISS][WARN] {name}: неизвестный статус {status}, пропускаем", proxy_tag=proxy_tag)
            continue

        rec_price = float(result.get("rec_price", 0) or 0)
        avg_sales = float(result.get("avg_sales", 0) or 0)

        recalculated_limits = evaluate_purchase_limits(
            name,
            avg_sales,
            purchased_lots,
            purchased_sum,
            time_lots,
            time_sum,
            has_db_entry=bool(purchase_stats.get("exists")),
            proxy_tag=proxy_tag,
        )
        if recalculated_limits is None:
            continue
        (quantity_max_allowed, sum_max_allowed, _, _, _, _) = recalculated_limits

        adjusted_rec_price = rec_price * 0.8697
        if adjusted_rec_price <= 0:
            print(f"[LISS][WARN] {name}: некорректная рек. цена ({rec_price})", proxy_tag=proxy_tag)
            continue

        if price <= 0:
            print(f"[LISS][WARN] {name}: некорректная цена лота ({price})", proxy_tag=proxy_tag)
            continue

        profit = adjusted_rec_price / price - 1
        if profit > config.LISS_MIN_PROFIT:
            print(
                f"[LISS] \"{name}\": {profit:.4f} выше {config.LISS_MIN_PROFIT} - "
                f"\033[92mapprove\033[0m (qty_left={quantity_max_allowed:.2f}, sum_left={sum_max_allowed:.2f})",
                proxy_tag=proxy_tag,
            )

            auto_search_and_buy(
                item_name=name,
                rec_price=rec_price,
                quantity_max_allowed=quantity_max_allowed,
                sum_max_allowed=sum_max_allowed,
            )
        else:
            print(f"[LISS][INFO] {name}: расчётная прибыль {profit:.4f} ниже порога", proxy_tag=proxy_tag)

        if stop_at is not None and time.time() >= stop_at:
            print("[LISS][TIMER] Таймер сработал во время обработки новочек, завершаем итерацию.")
            break

    priceAnalys.set_progress(None, None)


def run_refresh_cycle(refresh_seconds: int, orange: str, reset: str, stop_at: float) -> None:
    priceAnalys.set_proxy_tag(None)
    priceAnalys.set_progress(None, None)
    print(f"{orange}[LISS] Старт парсинга JSON прайс-листа{reset}")

    try:
        market_items = fetch_market_items()
    except (requests.RequestException, ValueError, json.JSONDecodeError) as exc:  # type: ignore[attr-defined]
        print(f"[LISS][ERROR] Не удалось загрузить список предметов: {exc}")
        return

    filtered_items = filter_by_keywords_and_price(market_items)
    known_items = load_known_items()
    processed_names, passed_filters, profit_passed, purchased_items = evaluate_known_items(
        filtered_items, known_items, stop_at
    )
    remaining_for_newcheck = max(len(filtered_items) - len(processed_names), 0)
    print(
        "[LISS][SUMMARY] Предчек завершён: "
        f"пройдено предчек {len(processed_names)}, "
        f"фильтры предчека {passed_filters}, "
        f"по прибыли {profit_passed}, "
        f"успешно куплено предметов {purchased_items}, "
        f"для новочек осталось {remaining_for_newcheck}"
    )

    if stop_at is not None and time.time() >= stop_at:
        print("[LISS][TIMER] Таймер сработал после предчека, запускаем следующий цикл без обработки новочек.")
        return

    process_new_items(filtered_items, processed_names, stop_at)


def main() -> None:
    print("[DEBUG] lis_search file:", lis_search.__file__)
    print("[DEBUG] search_lis_skins signature:", inspect.signature(lis_search.search_lis_skins))
    priceAnalys.init_db()
    priceAnalys.load_proxies_from_file()

    global LISS_ACCOUNTS
    LISS_ACCOUNTS = load_liss_accounts()
    init_purchase_dbs(LISS_ACCOUNTS)

    refresh_seconds = max(60, int(config.PRICE_REFRESH_MINUTES * 60))
    orange = "\033[38;5;208m"
    reset = "\033[0m"

    # Полный дамп (SQLite индекс) — первый раз готовим синхронно, потом обновляем в фоне
    global FULL_DUMP, ACTIVE_FULL_DB
    FULL_DUMP = FullDumpUpdater(root_dir=Path.cwd(), refresh_seconds=refresh_seconds)
    FULL_DUMP.ensure_initial_ready()
    ACTIVE_FULL_DB = FULL_DUMP.get_active_db_path()
    print(f"[LISS] Активный индекс полного дампа: {ACTIVE_FULL_DB.name}")
    # Готовим обновление в фоне для следующего цикла
    FULL_DUMP.trigger_async()

    next_refresh_start = time.time()

    while True:
        try:
            now = time.time()
            if now < next_refresh_start:
                time.sleep(next_refresh_start - now)

            iteration_started_at = time.time()

            # ВАЖНО: в начале итерации ждём завершения фонового обновления полного дампа
            # (если оно было запущено в прошлую итерацию).
            if FULL_DUMP is not None:
                FULL_DUMP.wait_update_complete()
                ACTIVE_FULL_DB = FULL_DUMP.get_active_db_path()

                # запускаем обновление полного дампа ДЛЯ СЛЕДУЮЩЕГО цикла СРАЗУ
                FULL_DUMP.trigger_async()

            stop_at = iteration_started_at + refresh_seconds
            run_refresh_cycle(refresh_seconds, orange, reset, stop_at)



            next_refresh_start = iteration_started_at + refresh_seconds

        except KeyboardInterrupt:
            print("[LISS] Остановка по запросу пользователя.")
            break
        except Exception as exc:  # защита от неожиданных сбоев
            print(f"[LISS][ERROR] Неожиданная ошибка в цикле: {exc}")
            next_refresh_start = time.time() + refresh_seconds


if __name__ == "__main__":
    main()
