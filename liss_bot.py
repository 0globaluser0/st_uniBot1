"""Асинхронные очереди и воркеры для Steam-парсинга и покупок LIS-SKINS."""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import config
import priceAnalys
from liss_api import LissApiClient


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


steam_parse_queue: asyncio.Queue[ItemForSteamParse] = asyncio.Queue()
liss_buy_queue: asyncio.Queue[PurchaseRequest] = asyncio.Queue()


def _extract_lot_price(lot: Dict[str, Any], fallback: float) -> Optional[float]:
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


async def steam_parse_worker(worker_name: str | int, account_name: str) -> None:
    """Воркер, который берёт задачи из steam_parse_queue и шлёт покупки в liss_buy_queue."""

    while True:
        item: ItemForSteamParse = await steam_parse_queue.get()
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

            steam_net_price = rec_price * 0.8697
            allowed_qty = max(1, math.floor(avg_sales * config.LISS_MAX_QTY_PERCENT_OF_WEEKLY))

            profitable_lots: List[Dict[str, Any]] = []
            for lot in item.lots:
                lot_price = _extract_lot_price(lot, item.current_lis_price)
                if lot_price is None:
                    continue
                hold_days = _extract_lot_hold_days(lot)
                if hold_days > config.LISS_MAX_HOLD_DAYS:
                    continue
                if lot_price < config.LISS_MIN_PRICE_USD or lot_price > config.LISS_MAX_PRICE_USD:
                    continue

                profit_ratio = (steam_net_price - lot_price) / lot_price
                if profit_ratio < config.LISS_MIN_PROFIT_RATIO:
                    continue

                lot_copy = dict(lot)
                lot_copy.setdefault("price_usd", lot_price)
                lot_copy.setdefault("steam_market_name", item.steam_market_name)
                lot_copy.setdefault("game_code", item.game_code)
                lot_copy.setdefault("lis_item_id", item.lis_item_id)
                lot_copy["profit_ratio"] = profit_ratio
                lot_copy["hold_days"] = hold_days
                profitable_lots.append(lot_copy)

            if not profitable_lots:
                print(f"[STEAM_WORKER {worker_name}] Нет подходящих лотов для {item.steam_market_name}")
                continue

            profitable_lots.sort(key=lambda x: x.get("price_usd", 0))
            selected_lots = profitable_lots[:allowed_qty]

            profit_estimate = sum(
                (steam_net_price - float(l.get("price_usd", 0))) * int(l.get("quantity", 1))
                for l in selected_lots
            )

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
