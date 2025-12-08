"""Асинхронные очереди и воркеры для Steam-парсинга и покупок LIS-SKINS."""

from __future__ import annotations

import asyncio
import itertools
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import config
import priceAnalys
from liss_api import LissApiClient, LissWebSocketClient, fetch_full_json_for_game


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


def _game_enabled(game_code: int) -> bool:
    return (game_code == 730 and config.LISS_ENABLE_CS2) or (
        game_code == 570 and config.LISS_ENABLE_DOTA2
    )


def _contains_excluded_keyword(name: str) -> bool:
    lower_name = (name or "").lower()
    return any(keyword.lower() in lower_name for keyword in config.LISS_EXCLUDED_KEYWORDS)


def _price_in_range(price: float) -> bool:
    return config.LISS_MIN_PRICE_USD <= price <= config.LISS_MAX_PRICE_USD


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
        print(
            f"[BLACKLIST] {item_name} в блэклисте до {expires_at_str}. "
            f"reason={reason}"
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
    fallback_price: Optional[float] = None,
) -> Tuple[List[Dict[str, Any]], float]:
    steam_net_price = rec_price * 0.8697
    allowed_qty = max(1, math.floor(avg_sales * config.LISS_MAX_QTY_PERCENT_OF_WEEKLY))

    profitable_lots: List[Dict[str, Any]] = []
    for lot in lots:
        lot_price = _extract_lot_price(lot, fallback_price or rec_price)
        if lot_price is None:
            continue
        hold_days = _extract_lot_hold_days(lot)
        if hold_days > config.LISS_MAX_HOLD_DAYS:
            continue
        if not _price_in_range(lot_price):
            continue

        profit_ratio = (steam_net_price - lot_price) / lot_price
        if profit_ratio < config.LISS_MIN_PROFIT_RATIO:
            continue

        lot_copy = dict(lot)
        lot_copy.setdefault("price_usd", lot_price)
        lot_copy.setdefault("steam_market_name", steam_market_name)
        lot_copy.setdefault("game_code", game_code)
        lot_copy.setdefault("lis_item_id", lis_item_id)
        lot_copy["profit_ratio"] = profit_ratio
        lot_copy["hold_days"] = hold_days
        profitable_lots.append(lot_copy)

    if not profitable_lots:
        return [], 0.0

    profitable_lots.sort(key=lambda x: x.get("price_usd", 0))
    selected_lots = profitable_lots[:allowed_qty]
    profit_estimate = sum(
        (steam_net_price - float(l.get("price_usd", 0))) * int(l.get("quantity", 1))
        for l in selected_lots
    )
    return selected_lots, profit_estimate


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


async def steam_parse_worker(worker_name: str | int, account_name: str) -> None:
    """Воркер, который берёт задачи из steam_parse_queue и шлёт покупки в liss_buy_queue."""

    while True:
        _, _, item = await steam_parse_queue.get()
        try:
            print(f"[STEAM_WORKER {worker_name}] Анализируем {item.steam_market_name}")
            analysis = await asyncio.to_thread(
                priceAnalys.analyse_item, item.steam_market_name, item.game_code
            )

            if analysis.get("status") != "ok":
                print(
                    f"[STEAM_WORKER {worker_name}] Пропускаем {item.steam_market_name}: "
                    f"status={analysis.get('status')}"
                )
                continue

            rec_price = float(analysis.get("rec_price") or 0.0)
            avg_sales = float(analysis.get("avg_sales") or 0.0)

            selected_lots, profit_estimate = _select_profitable_lots(
                item.lots,
                item.steam_market_name,
                item.game_code,
                item.lis_item_id,
                rec_price,
                avg_sales,
                fallback_price=item.current_lis_price,
            )

            if not selected_lots:
                print(
                    f"[STEAM_WORKER {worker_name}] Нет подходящих лотов для {item.steam_market_name}"
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
            print(
                f"[STEAM_WORKER {worker_name}] Отправлен запрос на покупку {len(selected_lots)} "
                f"лотов для {item.steam_market_name}, ожидаемая прибыль {profit_estimate:.2f}"
            )
        finally:
            steam_parse_queue.task_done()


async def liss_buy_worker(worker_name: str | int, client: LissApiClient) -> None:
    """Воркер, выполняющий покупки через LissApiClient с учётом задержек."""

    last_call_ts = 0.0
    loop = asyncio.get_running_loop()

    while True:
        request: PurchaseRequest = await liss_buy_queue.get()
        try:
            now = loop.time()
            wait_for = config.LISS_API_REQUEST_DELAY - (now - last_call_ts)
            if wait_for > 0:
                await asyncio.sleep(wait_for)

            print(
                f"[LISS_WORKER {worker_name}] Покупаем {len(request.selected_lots)} "
                f"лотов для {request.steam_market_name} (аккаунт {request.account_name})"
            )

            try:
                response = await client.buy_items(request.selected_lots)
                last_call_ts = loop.time()
            except Exception as e:
                print(f"[LISS_WORKER {worker_name}] Ошибка вызова buy_items: {e}")
                continue

            results_map = _normalize_purchase_results(response)
            now_dt = datetime.utcnow()

            for lot in request.selected_lots:
                lot_id = _lot_id(lot)
                result_entry = results_map.get(lot_id, {}) if lot_id else {}

                if not _is_successful_status(result_entry):
                    status = result_entry.get("status", "unknown")
                    print(
                        f"[LISS_WORKER {worker_name}] Не удалось купить {lot_id}: status={status}"
                    )
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
                    print(
                        f"[LISS_WORKER {worker_name}] Ошибка записи покупки в БД для {lot_id}: {e}"
                    )

                status = result_entry.get("status", "ok")
                print(
                    f"[LISS_WORKER {worker_name}] Куплен {request.steam_market_name} lot={lot_id} "
                    f"status={status} price={price_usd} qty={quantity}"
                )
        finally:
            liss_buy_queue.task_done()


def _lot_passes_basic_filters(
    steam_market_name: str, game_code: int, price_usd: float, hold_days: int
) -> bool:
    if not _game_enabled(game_code):
        return False
    if _contains_excluded_keyword(steam_market_name):
        return False
    if not _price_in_range(price_usd):
        return False
    if hold_days > config.LISS_MAX_HOLD_DAYS:
        return False
    return True


def _pick_cheapest_lot(lots: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    cheapest: Optional[Tuple[float, Dict[str, Any]]] = None
    for lot in lots:
        lot_price = _extract_lot_price(lot, math.inf)
        if lot_price is None:
            continue
        if cheapest is None or lot_price < cheapest[0]:
            cheapest = (lot_price, lot)
    return cheapest[1] if cheapest else None


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

    if steam_market_name is None or lot_price is None:
        return
    if not _lot_passes_basic_filters(steam_market_name, game_code, lot_price, hold_days):
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
            fallback_price=lot_price,
        )

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
            print(
                f"[CACHE_PURCHASE] Отправлен запрос на покупку {steam_market_name} "
                f"по кэшу rec_price={rec_price:.2f}"
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
    current_items = await _load_current_market_state(session)

    for (game_code, steam_market_name), lots in current_items.items():
        best_lot = _pick_cheapest_lot(lots)
        if best_lot is None:
            continue
        await _handle_candidate_lot(best_lot, account_name)

    print("[INIT] Начальный JSON обработан, переходим в режим WebSocket")
    return current_items


async def _load_current_market_state(session: Any) -> Dict[Tuple[int, str], List[Dict[str, Any]]]:
    enabled_games = []
    if config.LISS_ENABLE_CS2:
        enabled_games.append(730)
    if config.LISS_ENABLE_DOTA2:
        enabled_games.append(570)

    current_items: Dict[Tuple[int, str], List[Dict[str, Any]]] = {}

    async def _load_game(game_code: int) -> None:
        raw_items = await fetch_full_json_for_game(game_code, session=session)
        for entry in raw_items:
            steam_market_name = entry.get("name") or entry.get("steam_market_name")
            if not steam_market_name:
                continue

            try:
                lot_price = float(entry.get("price_usd"))
            except (TypeError, ValueError):
                continue
            hold_days = _extract_lot_hold_days(entry)
            if lot_price is None:
                continue

            lot = {
                "steam_market_name": steam_market_name,
                "game_code": int(entry.get("game_code") or game_code),
                "lis_item_id": entry.get("lis_item_id") or entry.get("id") or "",
                "price_usd": lot_price,
                "hold_days": hold_days,
                "raw": entry.get("raw") or entry,
            }

            if not _lot_passes_basic_filters(
                steam_market_name, lot["game_code"], lot["price_usd"], hold_days
            ):
                continue

            _store_lot_in_state(current_items, lot)

    await asyncio.gather(*(_load_game(game) for game in enabled_games))
    return current_items


async def _periodic_resync_state(
    account_name: str,
    session: Any,
    current_items: Dict[Tuple[int, str], List[Dict[str, Any]]],
    state_lock: asyncio.Lock,
) -> None:
    if config.LISS_JSON_RESYNC_MINUTES <= 0:
        return

    interval = config.LISS_JSON_RESYNC_MINUTES * 60

    while True:
        await asyncio.sleep(interval)
        try:
            snapshot = await _load_current_market_state(session)
        except Exception as e:
            print(f"[RESYNC] Ошибка загрузки снапшота: {e}")
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
) -> None:
    loop = asyncio.get_running_loop()

    while True:
        event = await ws_queue.get()
        try:
            event_type = (event.get("event") or "").strip()
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
                if event_type == "obtained_skin_deleted":
                    if lot_id is None:
                        continue
                    _remove_lot_from_state(current_items, key, lot_id)
                    continue

                if lot_price is None:
                    continue
                if not _lot_passes_basic_filters(
                    steam_market_name, game_code, lot_price, hold_days
                ):
                    continue

                current_best = _pick_cheapest_lot(current_items.get(key, []))
                current_best_price = _lot_price(current_best) if current_best else math.inf
                is_cheapest = lot_price < current_best_price

                lot = {
                    "steam_market_name": steam_market_name,
                    "game_code": game_code,
                    "lis_item_id": lot_id or event.get("id") or "",
                    "price_usd": float(lot_price),
                    "hold_days": hold_days,
                    "raw": event,
                }

                _store_lot_in_state(current_items, lot)

                if is_cheapest:
                    candidate_lot = lot

            if candidate_lot:
                is_recent = (loop.time() - event.get("received_ts", loop.time())) <= config.LISS_WS_PRIORITY_WINDOW_SEC
                await _handle_candidate_lot(candidate_lot, account_name, high_priority=is_recent)
        finally:
            ws_queue.task_done()


async def run_liss_market(account_name: str, api_key: str, session: Any) -> None:
    """Основной цикл: стартуем воркеры, загружаем JSON и слушаем WebSocket."""

    client = LissApiClient(api_key, account_name, session)
    ws_events: asyncio.Queue = asyncio.Queue()
    ws_client = LissWebSocketClient(api_key, ws_events)

    buyer_tasks = [
        asyncio.create_task(liss_buy_worker(idx + 1, client))
        for idx in range(config.LISS_MAX_LISS_BUYERS)
    ]
    parser_tasks = [
        asyncio.create_task(steam_parse_worker(idx + 1, account_name))
        for idx in range(config.LISS_MAX_STEAM_PARSERS)
    ]

    ws_task = asyncio.create_task(ws_client.run())
    ws_consumer_task: Optional[asyncio.Task] = None

    state_lock = asyncio.Lock()
    current_items = await _process_initial_json_snapshot(account_name, session)
    if ws_consumer_task is None:
        ws_consumer_task = asyncio.create_task(
            _websocket_consumer(account_name, ws_events, current_items, state_lock)
        )

    resync_task = asyncio.create_task(
        _periodic_resync_state(account_name, session, current_items, state_lock)
    )

    await asyncio.gather(ws_task, ws_consumer_task, resync_task, *buyer_tasks, *parser_tasks)
