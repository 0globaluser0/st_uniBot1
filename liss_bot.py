"""Асинхронные очереди и воркеры для Steam-парсинга и покупок LIS-SKINS."""

from __future__ import annotations

import asyncio
import itertools
import logging
import math
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import config
import priceAnalys
from liss_api import LissApiClient, LissWebSocketClient, fetch_full_json_for_game


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("liss_bot")

try:  # Optional dependency: prefer httpx, fallback to aiohttp
    import httpx
except Exception:  # pragma: no cover - optional dependency
    httpx = None  # type: ignore

try:
    import aiohttp
except Exception:  # pragma: no cover - optional dependency
    aiohttp = None  # type: ignore


@dataclass
class ItemForSteamParse:
    steam_market_name: str
    game_code: int
    lis_item_id: str
    current_lis_price: float
    lots: List[Dict[str, Any]]


@dataclass
class PurchaseRequest:
    steam_market_name: str
    game_code: int
    selected_lots: List[Dict[str, Any]]
    account_name: str
    target_rec_price: float
    avg_sales: float
    profit: float


steam_parse_queue: asyncio.PriorityQueue[Tuple[int, int, ItemForSteamParse]] = (
    asyncio.PriorityQueue()
)
liss_buy_queue: asyncio.Queue[PurchaseRequest] = asyncio.Queue()
_steam_parse_seq = itertools.count()
_last_high_priority_ts = 0.0
_high_priority_group = 0

_liss_buy_attempts: Dict[Tuple[str, str], int] = {}
_liss_buy_temp_locks: Dict[Tuple[str, str], float] = {}
_liss_buy_blacklist: Dict[Tuple[str, str], str] = {}
_account_price_floor: Dict[str, float] = {}
_account_locked_qty: Dict[str, int] = {}


def _reset_liss_state() -> None:
    """Очистить очереди и состояния между аккаунтами."""

    global steam_parse_queue, liss_buy_queue, _steam_parse_seq, _last_high_priority_ts, _high_priority_group

    steam_parse_queue = asyncio.PriorityQueue()
    liss_buy_queue = asyncio.Queue()
    _steam_parse_seq = itertools.count()
    _last_high_priority_ts = 0.0
    _high_priority_group = 0

    _liss_buy_attempts.clear()
    _liss_buy_temp_locks.clear()
    _liss_buy_blacklist.clear()
    _account_price_floor.clear()
    _account_locked_qty.clear()


def _extract_lot_price(lot: Dict[str, Any], fallback: Optional[float]) -> Optional[float]:
    for key in ("price_usd", "price", "usd_price"):
        if lot.get(key) is not None:
            try:
                return float(lot[key])
            except (TypeError, ValueError):
                continue
    return fallback


def _extract_lot_hold_days(lot: Dict[str, Any]) -> int:
    hold_value = lot.get("hold") or lot.get("hold_days")
    if hold_value is None:
        return 0
    try:
        return int(float(hold_value))
    except (TypeError, ValueError):
        return 0


def _lot_id(lot: Dict[str, Any]) -> Optional[str]:
    for key in ("lot_id", "lis_item_id", "item_id", "id", "custom_id"):
        value = lot.get(key)
        if value is not None:
            return str(value)
    return None


def _is_successful_status(entry: Dict[str, Any]) -> bool:
    status = str(entry.get("status", "")).lower()
    if "success" in status or status in {"ok", "purchased", "done"}:
        return True
    return bool(entry.get("purchase_id") or entry.get("item_asset_id"))


def _normalize_purchase_results(payload: Any) -> Dict[str, Dict[str, Any]]:
    if isinstance(payload, list):
        entries = payload
    elif isinstance(payload, dict):
        data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
        entries = data.get("items") or data.get("results") or data.get("result") or payload.get("items")
        if entries is None:
            entries = []
    else:
        entries = []

    mapping: Dict[str, Dict[str, Any]] = {}
    if not isinstance(entries, list):
        return mapping

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        lot_id = _lot_id(entry)
        if lot_id is None:
            continue
        mapping[lot_id] = entry
    return mapping


def _make_liss_key(account_name: str, lot: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    lot_id = _lot_id(lot)
    if lot_id is None:
        return None
    return account_name, str(lot_id)


def _cleanup_expired_temp_locks(now_ts: Optional[float] = None) -> None:
    if not _liss_buy_temp_locks:
        return

    now = now_ts or time.time()
    expired = [key for key, expires_at in _liss_buy_temp_locks.items() if expires_at <= now]
    for key in expired:
        _liss_buy_temp_locks.pop(key, None)


def _is_lot_blocked(account_name: str, lot: Dict[str, Any]) -> bool:
    _cleanup_expired_temp_locks()
    key = _make_liss_key(account_name, lot)
    if key is None:
        return False
    if key in _liss_buy_blacklist:
        return True
    lock_expires_at = _liss_buy_temp_locks.get(key)
    if lock_expires_at is None:
        return False
    if lock_expires_at > time.time():
        return True
    _liss_buy_temp_locks.pop(key, None)
    return False


def _filter_available_lots(lots: List[Dict[str, Any]], account_name: str) -> List[Dict[str, Any]]:
    return [lot for lot in lots if not _is_lot_blocked(account_name, lot)]


def _clear_lot_state(key: Optional[Tuple[str, str]]) -> None:
    if key is None:
        return
    _liss_buy_attempts.pop(key, None)
    _liss_buy_temp_locks.pop(key, None)
    _liss_buy_blacklist.pop(key, None)


def _is_ignorable_buy_error(status: str) -> bool:
    normalized = status.lower()
    keywords = [
        "недоступен",
        "выкуплен",
        "куплен",
        "не существует",
        "не найден",
        "price changed",
        "price has changed",
        "price_changed",
        "unavailable",
        "already purchased",
        "already bought",
        "not exist",
        "not found",
        "price change",
    ]
    return any(k in normalized for k in keywords)


def _register_buy_error(key: Optional[Tuple[str, str]], status: str) -> None:
    if key is None:
        return

    if _is_ignorable_buy_error(status):
        return

    attempts = _liss_buy_attempts.get(key, 0) + 1
    _liss_buy_attempts[key] = attempts

    if attempts >= config.LISS_BUY_MAX_RETRIES_PER_LOT:
        _liss_buy_blacklist[key] = status
        _liss_buy_temp_locks.pop(key, None)
        return

    _liss_buy_temp_locks[key] = time.time() + config.LISS_BUY_TEMP_LOCK_SECONDS


def _game_enabled(game_code: int) -> bool:
    return (game_code == 730 and config.LISS_ENABLE_CS2) or (
        game_code == 570 and config.LISS_ENABLE_DOTA2
    )


def _contains_excluded_keyword(name: str) -> bool:
    lower_name = (name or "").lower()
    return any(keyword.lower() in lower_name for keyword in config.LISS_EXCLUDED_KEYWORDS)


def _price_in_range(price: float, min_price: float) -> bool:
    return min_price <= price <= config.LISS_MAX_PRICE_USD


def _is_blacklisted(item_name: str) -> bool:
    bl = priceAnalys.get_blacklist_entry(item_name)
    if bl is None:
        return False

    expires_at_str = None
    for key in ("expires_at", "expired_at"):
        if key in bl.keys():
            expires_at_str = bl[key]
            break

    try:
        expires_at = datetime.fromisoformat(expires_at_str)
    except Exception:
        expires_at = None

    if expires_at is not None and expires_at > datetime.utcnow():
        reason = bl["reason"] if "reason" in bl.keys() else None
        logger.info(
            "[BLACKLIST] %s в блэклисте до %s. reason=%s",
            item_name,
            expires_at_str,
            reason,
        )
        return True

    priceAnalys.remove_from_blacklist(item_name)
    return False


def _get_actual_cached_price(item_name: str) -> Optional[Tuple[float, float]]:
    row = priceAnalys.get_cached_item(item_name)
    if row is None or row["rec_price"] is None:
        return None

    updated_at = row["updated_at"]
    try:
        dt_updated = datetime.fromisoformat(updated_at)
    except Exception:
        dt_updated = None

    if dt_updated is None:
        return None

    age_hours = (datetime.utcnow() - dt_updated).total_seconds() / 3600.0
    if age_hours >= config.ACTUAL_HOURS:
        return None

    return float(row["rec_price"]), float(row["avg_sales"] or 0.0)


def _select_profitable_lots(
    lots: Iterable[Dict[str, Any]],
    steam_market_name: str,
    game_code: int,
    lis_item_id: str,
    rec_price: float,
    avg_sales: float,
    account_name: str,
    fallback_price: Optional[float] = None,
    min_price_usd: Optional[float] = None,
) -> Tuple[List[Dict[str, Any]], float]:
    now_dt = datetime.utcnow()
    steam_net_price = rec_price * 0.8697
    effective_min_price = min_price_usd if min_price_usd is not None else _effective_min_price(account_name)

    remaining_qty, remaining_sum = priceAnalys.calculate_remaining_liss_limits(
        steam_market_name, account_name, avg_sales, now_dt
    )

    allowed_qty = math.floor(remaining_qty)
    if allowed_qty <= 0 or remaining_sum <= 0:
        return [], 0.0

    candidate_lots: List[Dict[str, Any]] = []
    for lot in lots:
        lot_price = _extract_lot_price(lot, fallback_price or rec_price)
        if lot_price is None:
            continue
        hold_days = _extract_lot_hold_days(lot)
        if hold_days > config.LISS_MAX_HOLD_DAYS:
            continue
        if not _price_in_range(lot_price, effective_min_price):
            continue

        lot_copy = dict(lot)
        lot_copy.setdefault("price_usd", float(lot_price))
        lot_copy.setdefault("steam_market_name", steam_market_name)
        lot_copy.setdefault("game_code", game_code)
        lot_copy.setdefault("lis_item_id", lis_item_id)
        lot_copy["hold_days"] = hold_days
        candidate_lots.append(lot_copy)

    if not candidate_lots:
        return [], 0.0

    candidate_lots.sort(key=lambda x: (x.get("price_usd", 0), x.get("hold_days", 0)))

    first_lot = candidate_lots[0]
    first_price = float(first_lot.get("price_usd", 0))
    first_profit_ratio = (steam_net_price - first_price) / first_price
    if first_profit_ratio < config.LISS_MIN_PROFIT_RATIO:
        logger.info(
            "[PROFIT_FILTER] %s (game=%s, acc=%s): cheapest lot is not profitable "
            "(price=%.2f, profit=%.4f < min=%.4f), skip item",
            steam_market_name,
            game_code,
            account_name,
            first_price,
            first_profit_ratio,
            config.LISS_MIN_PROFIT_RATIO,
        )
        return [], 0.0

    selected_lots: List[Dict[str, Any]] = []
    total_qty = 0
    total_sum = 0.0

    for lot in candidate_lots:
        lot_price = float(lot.get("price_usd", 0))
        profit_ratio = (steam_net_price - lot_price) / lot_price
        if profit_ratio < config.LISS_MIN_PROFIT_RATIO:
            continue

        lot["profit_ratio"] = profit_ratio
        quantity = int(lot.get("quantity", 1))
        lot_sum = lot_price * quantity

        if total_qty + quantity > allowed_qty:
            continue
        if total_sum + lot_sum > remaining_sum:
            continue

        selected_lots.append(lot)
        total_qty += quantity
        total_sum += lot_sum

    return selected_lots, total_sum


def _estimate_profit(lots: Iterable[Dict[str, Any]], rec_price: float) -> float:
    steam_net_price = rec_price * 0.8697
    return sum(
        (steam_net_price - float(l.get("price_usd", 0))) * int(l.get("quantity", 1))
        for l in lots
    )


def _normalize_search_results(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    entries: Any = None
    if isinstance(payload, dict):
        entries = payload.get("items") or payload.get("skins") or payload.get("results")
    if not isinstance(entries, list):
        return []
    normalized: List[Dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        normalized.append(entry)
    return normalized


async def search_and_select_lots(
    client: LissApiClient,
    steam_market_name: str,
    game_code: int,
    account_name: str,
    *,
    desired_qty: int,
    avg_sales: float,
    page_limit: int = 100,
    max_pages: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Ищет лоты через ``/v1/market/search`` и выбирает лучшие в рамках лимитов."""

    effective_min_price = _effective_min_price(account_name)
    lots: List[Dict[str, Any]] = []
    initial_price_bounds: Optional[Tuple[float, float]] = None
    offset = 0
    pages_fetched = 0

    while True:
        payload = await client.search_skins(game_code, steam_market_name, limit=page_limit, offset=offset)
        page_entries = _normalize_search_results(payload)

        for entry in page_entries:
            price_usd = _extract_lot_price(entry, None)
            if price_usd is None:
                continue

            hold_days = _extract_lot_hold_days(entry)
            name = (
                entry.get("steam_market_name")
                or entry.get("market_hash_name")
                or entry.get("market_hash")
                or entry.get("name")
                or steam_market_name
            )
            lot_game = int(entry.get("app_id") or entry.get("game") or game_code)

            passes_filters, _ = _lot_passes_basic_filters(
                name,
                lot_game,
                float(price_usd),
                hold_days,
                min_price_usd=effective_min_price,
            )
            if not passes_filters or _is_blacklisted(name):
                continue

            if initial_price_bounds is not None:
                min_price, max_price = initial_price_bounds
                if price_usd < min_price or price_usd > max_price:
                    continue

            lot: Dict[str, Any] = {
                "lot_id": entry.get("lot_id")
                or entry.get("lis_item_id")
                or entry.get("item_id")
                or entry.get("id"),
                "custom_id": entry.get("custom_id"),
                "asset_id": entry.get("asset_id") or entry.get("item_asset_id"),
                "name": name,
                "steam_market_name": name,
                "game_code": lot_game,
                "price_usd": float(price_usd),
                "hold_days": hold_days,
            }

            for extra_key in ("unlocked_at", "unlock_at", "unlock_dt"):
                if extra_key in entry:
                    lot[extra_key] = entry.get(extra_key)
                    break

            lots.append(lot)

        if initial_price_bounds is None and lots:
            prices = [float(l.get("price_usd", 0)) for l in lots]
            initial_price_bounds = (min(prices), max(prices))

        pages_fetched += 1
        if len(lots) >= desired_qty:
            break
        if max_pages is not None and pages_fetched >= max_pages:
            break

        has_more = bool(payload.get("has_more"))
        next_offset = payload.get("next_offset")
        if isinstance(next_offset, int):
            has_more = has_more or next_offset > offset
            offset = next_offset
        else:
            offset += page_limit
        if not has_more or not page_entries:
            break

    lots.sort(key=lambda x: (x.get("price_usd", math.inf), x.get("hold_days", math.inf)))

    now_dt = datetime.utcnow()
    remaining_qty, remaining_sum = priceAnalys.calculate_remaining_liss_limits(
        steam_market_name, account_name, avg_sales, now_dt
    )
    allowed_qty = min(int(math.floor(remaining_qty)), desired_qty)

    selected_lots: List[Dict[str, Any]] = []
    total_qty = 0
    total_sum = 0.0

    for lot in lots:
        lot_price = float(lot.get("price_usd", 0.0))
        quantity = int(lot.get("quantity", 1))
        lot_sum = lot_price * quantity

        if total_qty + quantity > allowed_qty:
            continue
        if total_sum + lot_sum > remaining_sum:
            continue

        lot_copy = dict(lot)
        unlock_dt = now_dt + timedelta(days=lot_copy.get("hold_days", 0))
        lot_copy["unlock_dt"] = unlock_dt.isoformat(timespec="seconds")

        selected_lots.append(lot_copy)
        total_qty += quantity
        total_sum += lot_sum

        if total_qty >= allowed_qty:
            break

    return selected_lots


async def _queue_steam_parse(item: ItemForSteamParse, high_priority: bool) -> None:
    global _last_high_priority_ts, _high_priority_group

    loop = asyncio.get_running_loop()
    priority: int

    if high_priority:
        now = loop.time()
        if now - _last_high_priority_ts > config.LISS_WS_PRIORITY_WINDOW_SEC:
            _high_priority_group -= 1
        priority = _high_priority_group
        _last_high_priority_ts = now
    else:
        priority = 1

    seq = next(_steam_parse_seq)
    await steam_parse_queue.put((priority, seq, item))


async def steam_parse_worker(
    worker_name: str | int, account_name: str, stop_event: asyncio.Event
) -> None:
    """Воркер, который берёт задачи из steam_parse_queue и шлёт покупки в liss_buy_queue."""

    while not stop_event.is_set():
        try:
            _, _, item = await asyncio.wait_for(steam_parse_queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue
        try:
            logger.info("[STEAM_WORKER %s] Анализируем %s", worker_name, item.steam_market_name)
            analysis = await asyncio.to_thread(
                priceAnalys.analyse_item, item.steam_market_name, item.game_code
            )

            if analysis.get("status") != "ok":
                logger.info(
                    "[STEAM_WORKER %s] Пропускаем %s: status=%s",
                    worker_name,
                    item.steam_market_name,
                    analysis.get("status"),
                )
                continue

            rec_price = float(analysis.get("rec_price") or 0.0)
            avg_sales = float(analysis.get("avg_sales") or 0.0)
            effective_min_price = _effective_min_price(account_name)

            selected_lots, profit_estimate = _select_profitable_lots(
                item.lots,
                item.steam_market_name,
                item.game_code,
                item.lis_item_id,
                rec_price,
                avg_sales,
                account_name,
                fallback_price=item.current_lis_price,
                min_price_usd=effective_min_price,
            )

            selected_lots = _filter_available_lots(selected_lots, account_name)
            profit_estimate = _estimate_profit(selected_lots, rec_price)

            if not selected_lots:
                logger.info(
                    "[STEAM_WORKER %s] Нет подходящих лотов для %s",
                    worker_name,
                    item.steam_market_name,
                )
                continue

            purchase_request = PurchaseRequest(
                steam_market_name=item.steam_market_name,
                game_code=item.game_code,
                selected_lots=selected_lots,
                account_name=account_name,
                target_rec_price=rec_price,
                avg_sales=avg_sales,
                profit=profit_estimate,
            )
            await liss_buy_queue.put(purchase_request)
            logger.info(
                "[STEAM_WORKER %s] Отправлен запрос на покупку %s лотов для %s, "
                "ожидаемая прибыль %.2f",
                worker_name,
                len(selected_lots),
                item.steam_market_name,
                profit_estimate,
            )
        finally:
            steam_parse_queue.task_done()


async def liss_buy_worker(
    worker_name: str | int, client: LissApiClient, stop_event: asyncio.Event
) -> None:
    """Воркер, выполняющий покупки через LissApiClient с учётом задержек."""

    last_call_ts = 0.0
    loop = asyncio.get_running_loop()

    while not stop_event.is_set():
        try:
            request: PurchaseRequest = await asyncio.wait_for(
                liss_buy_queue.get(), timeout=1.0
            )
        except asyncio.TimeoutError:
            continue
        try:
            _cleanup_expired_temp_locks()
            now = loop.time()
            wait_for = config.LISS_API_REQUEST_DELAY - (now - last_call_ts)
            if wait_for > 0:
                await asyncio.sleep(wait_for)

            logger.info(
                "[LISS_WORKER %s] Покупаем %s лотов для %s (аккаунт %s)",
                worker_name,
                len(request.selected_lots),
                request.steam_market_name,
                request.account_name,
            )

            try:
                response = await client.buy_items(request.selected_lots)
                last_call_ts = loop.time()
            except Exception as e:
                logger.exception(
                    "[LISS_WORKER %s] Ошибка вызова buy_items for %s: %s",
                    worker_name,
                    request.account_name,
                    e,
                )
                for lot in request.selected_lots:
                    key = _make_liss_key(request.account_name, lot)
                    _register_buy_error(key, str(e))
                continue

            results_map = _normalize_purchase_results(response)
            now_dt = datetime.utcnow()

            for lot in request.selected_lots:
                lot_id = _lot_id(lot)
                result_entry = results_map.get(lot_id, {}) if lot_id else {}
                key = _make_liss_key(request.account_name, lot)

                if not _is_successful_status(result_entry):
                    status = str(
                        result_entry.get("status")
                        or result_entry.get("message")
                        or result_entry.get("error")
                        or "unknown"
                    )
                    logger.info(
                        "[LISS_WORKER %s] Не удалось купить %s for %s: status=%s",
                        worker_name,
                        lot_id,
                        request.account_name,
                        status,
                    )
                    _register_buy_error(key, status)
                    continue

                quantity = int(lot.get("quantity", 1))
                price_usd = float(lot.get("price_usd", 0))
                sum_usd = price_usd * quantity

                priceAnalys.update_liss_limits_after_purchase(
                    request.steam_market_name,
                    request.account_name,
                    now_dt,
                    quantity,
                    sum_usd,
                )

                hold_days = _extract_lot_hold_days(lot)
                unlock_dt = now_dt + timedelta(days=hold_days)
                purchase_id = result_entry.get("purchase_id") or result_entry.get("id")
                asset_id = result_entry.get("item_asset_id") or result_entry.get("asset_id")

                try:
                    priceAnalys.insert_liss_purchase(
                        request.account_name,
                        lot_id or "",
                        lot.get("name") or request.steam_market_name,
                        request.steam_market_name,
                        request.game_code,
                        price_usd,
                        request.target_rec_price,
                        quantity,
                        now_dt.isoformat(timespec="seconds"),
                        unlock_dt.isoformat(timespec="seconds"),
                        hold_days,
                        asset_id or "",
                        custom_id=lot.get("custom_id") or purchase_id,
                    )
                except Exception as e:
                    logger.exception(
                        "[LISS_WORKER %s] Ошибка записи покупки в БД для %s: %s",
                        worker_name,
                        lot_id,
                        e,
                    )

                status = result_entry.get("status", "ok")
                _clear_lot_state(key)
                logger.info(
                    "[LISS_WORKER %s] Куплен %s lot=%s status=%s price=%.2f qty=%s",
                    worker_name,
                    request.steam_market_name,
                    lot_id,
                    status,
                    price_usd,
                    quantity,
                )
        finally:
            try:
                await _refresh_account_price_floor(request.account_name, client)
            except Exception as e:
                logger.exception("[PRICE_FLOOR] Не удалось обновить порог цены: %s", e)
            liss_buy_queue.task_done()


def _lot_passes_basic_filters(
    steam_market_name: str,
    game_code: int,
    price_usd: float,
    hold_days: int,
    *,
    min_price_usd: Optional[float] = None,
) -> Tuple[bool, Optional[str]]:
    if not _game_enabled(game_code):
        return False, "game_disabled"
    effective_min_price = min_price_usd if min_price_usd is not None else config.LISS_MIN_PRICE_USD
    if not _price_in_range(price_usd, effective_min_price):
        return False, "price_out_of_range"
    if _contains_excluded_keyword(steam_market_name):
        return False, "excluded_keyword"
    if hold_days > config.LISS_MAX_HOLD_DAYS:
        return False, "hold_too_long"
    return True, None


def _effective_min_price(account_name: str) -> float:
    return _account_price_floor.get(account_name, config.LISS_MIN_PRICE_USD)


async def _refresh_account_price_floor(account_name: str, client: LissApiClient) -> None:
    locked_qty = priceAnalys.count_liss_locked_purchases(account_name)
    _account_locked_qty[account_name] = locked_qty

    effective_min_price = config.LISS_MIN_PRICE_USD

    if locked_qty >= config.LISS_LOCKED_ITEMS_WARNING_THRESHOLD:
        try:
            balance_usd = await client.get_balance()
        except Exception as e:
            logger.exception("[PRICE_FLOOR] Не удалось получить баланс %s: %s", account_name, e)
            balance_usd = 0.0

        free_slots = config.LISS_MAX_INVENTORY_SLOTS - locked_qty
        if free_slots <= 0:
            free_slots = 1

        dynamic_min_price = math.ceil((balance_usd / free_slots) * 100) / 100.0
        effective_min_price = max(config.LISS_MIN_PRICE_USD, dynamic_min_price)

    previous_price = _account_price_floor.get(account_name)
    _account_price_floor[account_name] = effective_min_price

    if previous_price != effective_min_price:
        logger.info(
            "[PRICE_FLOOR] account=%s locked_qty=%s min_price=%.2f",
            account_name,
            locked_qty,
            effective_min_price,
        )


def _pick_cheapest_lot(lots: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    cheapest: Optional[Tuple[float, int, Dict[str, Any]]] = None
    for lot in lots:
        lot_price = _extract_lot_price(lot, math.inf)
        if lot_price is None:
            continue
        hold_days = _extract_lot_hold_days(lot)
        if hold_days > config.LISS_MAX_HOLD_DAYS:
            continue
        if cheapest is None or lot_price < cheapest[0] or (
            lot_price == cheapest[0] and hold_days < cheapest[1]
        ):
            cheapest = (lot_price, hold_days, lot)
    return cheapest[2] if cheapest else None


def _lot_price(lot: Dict[str, Any]) -> Optional[float]:
    return _extract_lot_price(lot, None) if lot is not None else None


def _store_lot_in_state(
    current_items: Dict[Tuple[int, str], List[Dict[str, Any]]],
    lot: Dict[str, Any],
) -> None:
    key = (int(lot.get("game_code") or 0), lot.get("steam_market_name") or lot.get("name"))
    if key[1] is None:
        return

    lots = current_items.setdefault(key, [])
    lot_id = _lot_id(lot)

    if lot_id is None:
        lots.append(lot)
        return

    for idx, existing in enumerate(lots):
        if _lot_id(existing) == lot_id:
            lots[idx] = lot
            break
    else:
        lots.append(lot)


def _remove_lot_from_state(
    current_items: Dict[Tuple[int, str], List[Dict[str, Any]]], key: Tuple[int, str], lot_id: str
) -> bool:
    lots = current_items.get(key)
    if not lots:
        return False

    new_lots = [lot for lot in lots if _lot_id(lot) != lot_id]
    if len(new_lots) == len(lots):
        return False

    if new_lots:
        current_items[key] = new_lots
    else:
        current_items.pop(key, None)
    return True


async def _handle_candidate_lot(
    lot: Dict[str, Any],
    account_name: str,
    *,
    high_priority: bool = False,
) -> None:
    steam_market_name = lot.get("steam_market_name") or lot.get("name")
    game_code = int(lot.get("game_code") or 0)
    lot_price = _lot_price(lot)
    hold_days = _extract_lot_hold_days(lot)

    effective_min_price = _effective_min_price(account_name)
    if steam_market_name is None or lot_price is None:
        return
    passes_filters, reason = _lot_passes_basic_filters(
        steam_market_name, game_code, lot_price, hold_days, min_price_usd=effective_min_price
    )
    if not passes_filters:
        logger.info(
            "[FILTER] account=%s skip=%s price=%.2f hold=%s reason=%s",
            account_name,
            steam_market_name,
            lot_price,
            hold_days,
            reason,
        )
        return
    if _is_blacklisted(steam_market_name):
        return

    cached = _get_actual_cached_price(steam_market_name)
    if cached is not None:
        rec_price, avg_sales = cached
        selected_lots, profit_estimate = _select_profitable_lots(
            [lot],
            steam_market_name,
            game_code,
            str(lot.get("lis_item_id") or lot.get("id") or ""),
            rec_price,
            avg_sales,
            account_name,
            fallback_price=lot_price,
            min_price_usd=effective_min_price,
        )

        selected_lots = _filter_available_lots(selected_lots, account_name)
        profit_estimate = _estimate_profit(selected_lots, rec_price)

        if selected_lots:
            purchase_request = PurchaseRequest(
                steam_market_name=steam_market_name,
                game_code=game_code,
                selected_lots=selected_lots,
                account_name=account_name,
                target_rec_price=rec_price,
                avg_sales=avg_sales,
                profit=profit_estimate,
            )
            await liss_buy_queue.put(purchase_request)
            logger.info(
                "[CACHE_PURCHASE] Отправлен запрос на покупку %s по кэшу rec_price=%.2f",
                steam_market_name,
                rec_price,
            )
            return

    item_for_parse = ItemForSteamParse(
        steam_market_name=steam_market_name,
        game_code=game_code,
        lis_item_id=str(lot.get("lis_item_id") or lot.get("id") or ""),
        current_lis_price=float(lot_price),
        lots=[lot],
    )
    await _queue_steam_parse(item_for_parse, high_priority=high_priority)


async def _process_initial_json_snapshot(
    account_name: str,
    session: Any,
) -> Dict[Tuple[int, str], List[Dict[str, Any]]]:
    current_items = await _load_current_market_state(account_name, session)

    for (game_code, steam_market_name), lots in current_items.items():
        best_lot = _pick_cheapest_lot(lots)
        if best_lot is None:
            continue
        await _handle_candidate_lot(best_lot, account_name)

    logger.info("[INIT] Начальный JSON обработан, переходим в режим WebSocket")
    return current_items


async def _load_current_market_state(
    account_name: str, session: Any
) -> Dict[Tuple[int, str], List[Dict[str, Any]]]:
    enabled_games = []
    if config.LISS_ENABLE_CS2:
        enabled_games.append(730)
    if config.LISS_ENABLE_DOTA2:
        enabled_games.append(570)

    current_items: Dict[Tuple[int, str], List[Dict[str, Any]]] = {}
    effective_min_price = _effective_min_price(account_name)

    async def _load_game(game_code: int) -> None:
        raw_items = await fetch_full_json_for_game(game_code, session=session)
        total_items = 0
        skipped_items = 0
        passed_items = 0

        for entry in raw_items:
            if not isinstance(entry, dict):
                continue

            steam_market_name = entry.get("steam_market_name") or entry.get("name")
            if not steam_market_name:
                continue

            try:
                price = float(entry.get("price_usd"))
            except (TypeError, ValueError):
                continue

            hold_days = _extract_lot_hold_days(entry)
            total_items += 1

            if total_items % 10000 == 0:
                logger.info(
                    "[FILTER_PROGRESS] account=%s game=%s processed=%d",
                    account_name,
                    game_code,
                    total_items,
                )

            passes_filters, reason = _lot_passes_basic_filters(
                steam_market_name,
                int(entry.get("app_id") or entry.get("game_code") or game_code),
                price,
                hold_days,
                min_price_usd=effective_min_price,
            )
            if not passes_filters:
                skipped_items += 1
                continue

            passed_items += 1
            try:
                count = int(entry.get("count") or 0)
            except (TypeError, ValueError):
                count = 0

            pseudo_lot = {
                "steam_market_name": steam_market_name,
                "name": steam_market_name,
                "game_code": int(entry.get("app_id") or entry.get("game_code") or game_code),
                "price_usd": price,
                "count": count,
                "hold_days": hold_days,
                "raw": entry,
            }
            _store_lot_in_state(current_items, pseudo_lot)

        logger.info(
            "[FILTER_SUMMARY] account=%s game=%s total_items=%d passed=%d skipped=%d",
            account_name,
            game_code,
            total_items,
            passed_items,
            skipped_items,
        )

    start = time.perf_counter()
    await asyncio.gather(*(_load_game(game) for game in enabled_games))
    end = time.perf_counter()
    logger.info(
        "[SNAPSHOT_DONE] account=%s games=%s elapsed=%.2f sec",
        account_name,
        enabled_games,
        end - start,
    )
    return current_items


async def _periodic_resync_state(
    account_name: str,
    session: Any,
    current_items: Dict[Tuple[int, str], List[Dict[str, Any]]],
    state_lock: asyncio.Lock,
    stop_event: asyncio.Event,
) -> None:
    if config.LISS_JSON_RESYNC_MINUTES <= 0:
        return

    interval = config.LISS_JSON_RESYNC_MINUTES * 60

    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
            break
        except asyncio.TimeoutError:
            pass
        try:
            snapshot = await _load_current_market_state(account_name, session)
        except Exception as e:
            logger.exception("[RESYNC] Ошибка загрузки снапшота: %s", e)
            continue

        candidates: List[Dict[str, Any]] = []

        async with state_lock:
            # удаляем исчезнувшие предметы
            for key in list(current_items.keys()):
                if key not in snapshot:
                    current_items.pop(key, None)

            for key, new_lots in snapshot.items():
                prev_best = _pick_cheapest_lot(current_items.get(key, []))
                new_best = _pick_cheapest_lot(new_lots)
                current_items[key] = new_lots

                prev_price = _lot_price(prev_best) if prev_best else math.inf
                new_price = _lot_price(new_best) if new_best else None

                if new_best is not None and new_price is not None and new_price < prev_price:
                    candidates.append(new_best)

        for lot in candidates:
            await _handle_candidate_lot(lot, account_name)


async def _websocket_consumer(
    account_name: str,
    ws_queue: asyncio.Queue,
    current_items: Dict[Tuple[int, str], List[Dict[str, Any]]],
    state_lock: asyncio.Lock,
    stop_event: asyncio.Event,
) -> None:
    loop = asyncio.get_running_loop()

    while not stop_event.is_set():
        try:
            event = await asyncio.wait_for(ws_queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue
        try:
            steam_market_name = event.get("steam_market_name")
            game_code = int(event.get("game_code") or 0)
            try:
                lot_price = float(event.get("price_usd"))
            except (TypeError, ValueError):
                lot_price = None
            hold_days = _extract_lot_hold_days(event)
            lot_id = _lot_id(event)
            if steam_market_name is None:
                continue

            key = (game_code, steam_market_name)
            candidate_lot: Optional[Dict[str, Any]] = None

            async with state_lock:
                prev_best = _pick_cheapest_lot(current_items.get(key, []))
                prev_best_price = _lot_price(prev_best)
                current_best_price = prev_best_price if prev_best_price is not None else math.inf

                if lot_price is not None and lot_price < current_best_price:
                    new_best = {
                        "steam_market_name": steam_market_name,
                        "game_code": game_code,
                        "lis_item_id": lot_id or event.get("id") or "",
                        "price_usd": float(lot_price),
                        "hold_days": hold_days,
                        "raw": event,
                    }
                    current_items[key] = [new_best]
                    candidate_lot = new_best

            if candidate_lot:
                is_recent = (loop.time() - event.get("received_ts", loop.time())) <= config.LISS_WS_PRIORITY_WINDOW_SEC
                await _handle_candidate_lot(candidate_lot, account_name, high_priority=is_recent)
        finally:
            ws_queue.task_done()


async def run_liss_market(
    account_name: str, api_key: str, session: Any, stop_event: asyncio.Event
) -> None:
    """Основной цикл: стартуем воркеры, загружаем JSON и слушаем WebSocket."""

    client = LissApiClient(api_key, account_name, session)
    ws_events: asyncio.Queue = asyncio.Queue()
    ws_client = LissWebSocketClient(api_key, ws_events)

    await _refresh_account_price_floor(account_name, client)

    buyer_tasks = [
        asyncio.create_task(liss_buy_worker(idx + 1, client, stop_event))
        for idx in range(config.LISS_MAX_LISS_BUYERS)
    ]
    parser_tasks = [
        asyncio.create_task(steam_parse_worker(idx + 1, account_name, stop_event))
        for idx in range(config.LISS_MAX_STEAM_PARSERS)
    ]

    ws_task = asyncio.create_task(ws_client.run())

    state_lock = asyncio.Lock()
    current_items = await _process_initial_json_snapshot(account_name, session)
    ws_consumer_task = asyncio.create_task(
        _websocket_consumer(account_name, ws_events, current_items, state_lock, stop_event)
    )

    resync_task = asyncio.create_task(
        _periodic_resync_state(account_name, session, current_items, state_lock, stop_event)
    )

    stop_waiter = asyncio.create_task(stop_event.wait())

    try:
        done, _ = await asyncio.wait(
            [stop_waiter, ws_task], return_when=asyncio.FIRST_COMPLETED
        )
        if ws_task in done and not stop_event.is_set():
            exc = ws_task.exception()
            if exc:
                logger.exception("[WS] Работа WebSocket завершилась ошибкой: %s", exc)
            else:
                logger.info("[WS] Работа WebSocket завершилась без ошибок")
            stop_event.set()

        await stop_event.wait()
    finally:
        stop_event.set()
        tasks_to_cancel = [
            ws_task,
            ws_consumer_task,
            resync_task,
            *buyer_tasks,
            *parser_tasks,
            stop_waiter,
        ]
        for task in tasks_to_cancel:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks_to_cancel, return_exceptions=True)


@asynccontextmanager
async def _create_http_session():
    """Создать HTTP-сессию для LIS-SKINS (httpx -> aiohttp)."""

    if httpx is not None:
        async with httpx.AsyncClient(timeout=config.HTTP_TIMEOUT) as client:
            yield client
        return

    if aiohttp is not None:
        timeout = aiohttp.ClientTimeout(total=config.HTTP_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            yield session
        return

    raise RuntimeError("Нужен httpx или aiohttp для работы с LIS-SKINS")


def load_liss_accounts(path: Optional[str] = None) -> List[Tuple[str, str]]:
    """Прочитать файл аккаунтов и вернуть список пар (api_key, account_name)."""

    filename = path or config.LISS_ACCOUNTS_FILE
    accounts: List[Tuple[str, str]] = []

    try:
        with open(filename, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except FileNotFoundError:
        logger.error("[ACCOUNTS] Файл %s не найден", filename)
        return accounts

    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" not in line:
            logger.warning("[ACCOUNTS] Строка без разделителя ':' пропущена: %s", line)
            continue
        api_key, account_name = line.split(":", 1)
        api_key = api_key.strip()
        account_name = account_name.strip()
        if not api_key or not account_name:
            logger.warning("[ACCOUNTS] Пустой api_key или account_name в строке: %s", line)
            continue
        accounts.append((api_key, account_name))

    return accounts


async def run_for_account(
    api_key: str, account_name: str, *, run_seconds: Optional[float] = None
) -> None:
    """Отработать один аккаунт LIS последовательно и корректно завершить."""

    _reset_liss_state()
    stop_event = asyncio.Event()

    logger.info("[ACCOUNT] Старт аккаунта %s", account_name)

    async with _create_http_session() as session:
        runner_task = asyncio.create_task(
            run_liss_market(account_name, api_key, session, stop_event)
        )

        timer_task: Optional[asyncio.Task] = None
        wait_tasks = [runner_task]

        if run_seconds is not None and run_seconds > 0:
            timer_task = asyncio.create_task(asyncio.sleep(run_seconds))
            wait_tasks.append(timer_task)

        try:
            done, _ = await asyncio.wait(wait_tasks, return_when=asyncio.FIRST_COMPLETED)
            if runner_task in done:
                exc = runner_task.exception()
                if exc:
                    raise exc
            if timer_task is not None and timer_task in done:
                logger.info(
                    "[MAIN] Таймер %.0fs для аккаунта %s истёк, завершаем",
                    run_seconds,
                    account_name,
                )
        finally:
            stop_event.set()
            if timer_task is not None:
                timer_task.cancel()
            to_wait = [runner_task]
            if timer_task is not None:
                to_wait.append(timer_task)
            await asyncio.gather(*to_wait, return_exceptions=True)

    logger.info("[ACCOUNT] Остановка аккаунта %s", account_name)


async def main() -> None:
    accounts = load_liss_accounts()
    if not accounts:
        logger.warning("[MAIN] Нет аккаунтов для обработки")
        return

    runtime = config.LISS_ACCOUNT_RUNTIME_SECONDS
    per_account_seconds = runtime if runtime > 0 else None

    for api_key, account_name in accounts:
        logger.info("[MAIN] Запуск обработки аккаунта %s", account_name)
        try:
            await run_for_account(api_key, account_name, run_seconds=per_account_seconds)
        except Exception as e:
            logger.exception("[MAIN] Ошибка при обработке %s: %s", account_name, e)


if __name__ == "__main__":
    asyncio.run(main())
