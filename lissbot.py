"""Утилита для предварительного анализа лотов lis-skins.

Скрипт скачивает список предметов, применяет набор фильтров и выводит
сообщения о готовности лотов к дальнейшему анализу (заглушки вместо
реальных покупок/парсинга Steam).
"""

import json
import time
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote

import requests

import config
import priceAnalys

MARKET_URL = "https://lis-skins.com/market_export_json/csgo.json"
STEAM_BASE_URL = "https://steamcommunity.com/market/listings/730/"


def build_steam_url(name: str) -> str:
    """Формирует ссылку на страницу предмета в Steam."""

    return f"{STEAM_BASE_URL}{quote(name)}"


def is_blacklisted(name: str) -> bool:
    """Возвращает True, если предмет в актуальном блэклисте.

    Если срок действия записи истёк, запись удаляется.
    """

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
        SELECT item_name, rec_price, avg_sales, purchased_lots, purchased_sum, updated_at
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
        })
    return items


def within_purchase_limits(avg_sales: float, purchased_lots: float, purchased_sum: float) -> bool:
    if avg_sales <= 0:
        allowed_lots = float("inf")
    else:
        allowed_lots = avg_sales * (config.LISS_QUANTITY_PERCENT / 100.0)

    return purchased_lots < allowed_lots and purchased_sum < config.LISS_SUM_LIMIT


def evaluate_known_items(
    market_items: List[Dict[str, object]],
    known_items: List[Dict[str, object]],
    stop_at: Optional[float] = None,
) -> Tuple[List[str], int, int]:
    market_map = {item["name"]: item for item in market_items}
    candidates = [item for item in known_items if item["name"] in market_map]
    processed: List[str] = []
    passed_filters = 0
    profit_passed = 0

    green = "\033[92m"
    reset = "\033[0m"

    total = len(candidates)
    for idx, known in enumerate(candidates, start=1):
        priceAnalys.set_progress(idx, total)
        name = known["name"]
        market_item = market_map.get(name)

        processed.append(name)

        if not within_purchase_limits(
            known["avg_sales"], known["purchased_lots"], known["purchased_sum"]
        ):
            continue

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
                f"{green}approve{reset}"
            )
            print("[LISS] предчек: предмет прошел фильтры и готов к парсингу id")
            profit_passed += 1
        else:
            passed_filters += 1

        if stop_at is not None and time.time() >= stop_at:
            print("[LISS][TIMER] Таймер сработал во время проверки известных предметов, завершаем итерацию.")
            break

    priceAnalys.set_progress(None, None)
    return processed, passed_filters, profit_passed


def get_purchase_stats(name: str) -> Tuple[float, float, float]:
    """Возвращает (avg_sales, purchased_lots, purchased_sum)."""

    conn = priceAnalys.get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT avg_sales, purchased_lots, purchased_sum FROM steam_items WHERE item_name = ?",
        (name,),
    )
    row = cur.fetchone()
    conn.close()
    if row is None:
        return 0.0, 0.0, 0.0
    return float(row["avg_sales"] or 0), float(row["purchased_lots"] or 0), float(row["purchased_sum"] or 0)


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

        avg_sales, purchased_lots, purchased_sum = get_purchase_stats(name)
        if avg_sales > 0 and not within_purchase_limits(
            avg_sales, purchased_lots, purchased_sum
        ):
            continue
        if purchased_sum >= config.LISS_SUM_LIMIT:
            continue

        targets.append(item)

    total = len(targets)
    for idx, item in enumerate(targets, start=1):
        priceAnalys.set_progress(idx, total)
        name = item["name"]
        price = float(item["price"])

        steam_url = build_steam_url(name)
#         print(
#            f"[LISS] новочек: {name} прошел фильтры, запускаем парсинг Steam (price={price:.2f})"
#       )

        result = priceAnalys.parsing_steam_sales(
            steam_url, log_blacklist_reason=False
        )
        status = result.get("status")
        proxy_tag = result.get("proxy_tag")

        if status == "invalid_link":
            print(
                f"[LISS][WARN] {name}: некорректная ссылка {steam_url}",
                proxy_tag=proxy_tag,
            )
            continue
        if status == "dota_soon":
            print(
                f"[LISS][INFO] {name}: анализ Dota пока не поддерживается",
                proxy_tag=proxy_tag,
            )
            continue
        if status == "blacklist":
            print(
                f"[LISS][INFO] {name}: в блэклисте ({result.get('reason')})",
                proxy_tag=proxy_tag,
            )
            continue
        if status == "error":
            print(
                f"[LISS][ERROR] {name}: {result.get('message')}",
                proxy_tag=proxy_tag,
            )
            continue
        if status != "ok":
            print(
                f"[LISS][WARN] {name}: неизвестный статус {status}, пропускаем",
                proxy_tag=proxy_tag,
            )
            continue

        rec_price = float(result.get("rec_price", 0) or 0)
        avg_sales = float(result.get("avg_sales", 0) or 0)

        if not within_purchase_limits(avg_sales, purchased_lots, purchased_sum):
            print(
                f"[LISS][INFO] {name}: достигнут лимит покупок",
                proxy_tag=proxy_tag,
            )
            continue

        adjusted_rec_price = rec_price * 0.8697
        if adjusted_rec_price <= 0:
            print(
                f"[LISS][WARN] {name}: некорректная рек. цена ({rec_price})",
                proxy_tag=proxy_tag,
            )
            continue

        if price <= 0:
            print(
                f"[LISS][WARN] {name}: некорректная цена лота ({price})",
                proxy_tag=proxy_tag,
            )
            continue

        profit = adjusted_rec_price / price - 1
        if profit > config.LISS_MIN_PROFIT:
            print(
                f"[LISS] \"{name}\": {profit:.4f} выше {config.LISS_MIN_PROFIT} - "
                "\033[92mapprove\033[0m",
                proxy_tag=proxy_tag,
            )
        else:
            print(
                f"[LISS][INFO] {name}: расчётная прибыль {profit:.4f} ниже порога",
                proxy_tag=proxy_tag,
            )

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
    processed_names, passed_filters, profit_passed = evaluate_known_items(
        filtered_items, known_items, stop_at
    )
    remaining_for_newcheck = max(len(filtered_items) - len(processed_names), 0)
    print(
        "[LISS][SUMMARY] Предчек завершён: "
        f"пройдено предчек {len(processed_names)}, "
        f"фильтры предчека {passed_filters}, "
        f"по прибыли {profit_passed}, "
        f"для новочек осталось {remaining_for_newcheck}"
    )
    if stop_at is not None and time.time() >= stop_at:
        print("[LISS][TIMER] Таймер сработал после предчека, запускаем следующий цикл без обработки новочек.")
        return

    process_new_items(filtered_items, processed_names, stop_at)


def main() -> None:
    priceAnalys.init_db()
    priceAnalys.load_proxies_from_file()

    refresh_seconds = max(60, int(config.PRICE_REFRESH_MINUTES * 60))
    orange = "\033[38;5;208m"
    reset = "\033[0m"

    next_refresh_start = time.time()

    while True:
        try:
            now = time.time()
            if now < next_refresh_start:
                time.sleep(next_refresh_start - now)

            iteration_started_at = time.time()
            stop_at = iteration_started_at + refresh_seconds
            run_refresh_cycle(refresh_seconds, orange, reset, stop_at)

            next_refresh_start = iteration_started_at + refresh_seconds
        except KeyboardInterrupt:
            print("[LISS] Остановка по запросу пользователя.")
            break
        except Exception as exc:  # pragma: no cover - защита от неожиданных сбоев
            print(f"[LISS][ERROR] Неожиданная ошибка в цикле: {exc}")
            next_refresh_start = time.time() + refresh_seconds


if __name__ == "__main__":
    main()
