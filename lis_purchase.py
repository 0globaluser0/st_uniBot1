"""Покупка лотов на lis-skins по ТЗ из запроса пользователя.

Модуль реализует полный цикл:
- отправка запроса /v1/market/buy с учётом всех правил повторных попыток;
- обработка кодов 400/422/429 и таймаутов;
- запросы /v1/market/check-availability и /v1/market/info при необходимости;
- фильтрация успешно купленных лотов и запись результатов в БД.

Поток данных построен вокруг элементов, полученных из /v1/market/search.
Каждый элемент должен включать минимум поля:
    id, name, price, unlock_at, created_at, item_float, hold, stickers (list)
"""

from __future__ import annotations

import random
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests

import config
from lis_api import _load_liss_accounts
import priceAnalys


BUY_URL = "https://api.lis-skins.com/v1/market/buy"
INFO_URL = "https://api.lis-skins.com/v1/market/info"
CHECK_AVAILABILITY_URL = "https://api.lis-skins.com/v1/market/check-availability"
BALANCE_URL = "https://api.lis-skins.com/v1/user/balance"


@dataclass
class PurchaseResult:
    success_items: List[Dict[str, Any]]
    skipped_items: List[Dict[str, Any]]
    account: str
    custom_id: str


class AccountSkipError(RuntimeError):
    """Ошибка, означающая необходимость перейти к следующему аккаунту."""


def _generate_custom_id() -> str:
    return f"{random.randint(1_000_000, 9_999_999)}"


def _request_with_retry(method: str, url: str, *, headers: Dict[str, str], **kwargs: Any) -> requests.Response:
    while True:
        try:
            resp = requests.request(method, url, headers=headers, timeout=config.HTTP_TIMEOUT, **kwargs)
        except requests.exceptions.Timeout:
            raise

        if resp.status_code == 429:
            time.sleep(2)
            continue
        return resp


def _send_buy_request(
    account: "lis_api.LisAccount", ids: Sequence[int], max_price: float, custom_id: str
) -> requests.Response:
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {account.api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "ids": list(ids),
        "partner": account.partner,
        "token": account.token,
        "max_price": round(max_price, 2),
        "custom_id": custom_id,
        "skip_unavailable": True,
    }
    return _request_with_retry("POST", BUY_URL, headers=headers, json=payload)


def _check_availability(account: "lis_api.LisAccount", ids: Sequence[int]) -> Optional[Dict[str, float]]:
    headers = {"Accept": "application/json", "Authorization": f"Bearer {account.api_key}"}
    resp = _request_with_retry("GET", CHECK_AVAILABILITY_URL, headers=headers, params={"ids": list(ids)})
    if resp.status_code not in (200, 201):
        print(f"[LISS][WARN] check-availability вернул {resp.status_code}, пропускаем лоты")
        return None

    data = resp.json().get("data") or {}
    available = data.get("available_skins") or {}
    try:
        return {int(k): float(v) for k, v in available.items()}
    except Exception:
        return None


def _get_purchase_info(account: "lis_api.LisAccount", custom_ids: Iterable[str]) -> Optional[List[Dict[str, Any]]]:
    headers = {"Accept": "application/json", "Authorization": f"Bearer {account.api_key}"}
    resp = _request_with_retry("GET", INFO_URL, headers=headers, params={"custom_ids": list(custom_ids)})
    if resp.status_code not in (200, 201):
        return None
    data = resp.json().get("data") or []
    if not isinstance(data, list):
        return None
    return data


def _ensure_purchase_db(account_name: str) -> sqlite3.Connection:
    db_path = f"{account_name}_liss_purchase.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS purchases (
            id INTEGER,
            name TEXT,
            price REAL,
            unlock_at TEXT,
            created_at TEXT,
            item_float REAL,
            hold INTEGER,
            sticker1 TEXT, wear1 REAL,
            sticker2 TEXT, wear2 REAL,
            sticker3 TEXT, wear3 REAL,
            sticker4 TEXT, wear4 REAL,
            sticker5 TEXT, wear5 REAL,
            custom_id TEXT,
            status TEXT,
            return_reason TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stats (
            key TEXT PRIMARY KEY,
            value REAL
        )
        """
    )
    conn.commit()
    return conn


def _record_purchases(account_name: str, items: List[Dict[str, Any]]) -> None:
    conn = _ensure_purchase_db(account_name)
    cur = conn.cursor()
    for item in items:
        stickers = item.get("stickers") or []
        stickers = stickers[:5] + [None] * (5 - len(stickers))
        flat: List[Optional[Any]] = []
        for st in stickers:
            if st:
                flat.extend([st.get("name"), st.get("wear")])
            else:
                flat.extend([None, None])
        cur.execute(
            """
            INSERT INTO purchases (
                id, name, price, unlock_at, created_at, item_float, hold,
                sticker1, wear1, sticker2, wear2, sticker3, wear3,
                sticker4, wear4, sticker5, wear5, custom_id, status, return_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item.get("id"),
                item.get("name"),
                item.get("price"),
                item.get("unlock_at"),
                item.get("created_at"),
                item.get("item_float"),
                item.get("hold"),
                *flat,
                item.get("custom_id"),
                item.get("status"),
                item.get("return_reason"),
            ),
        )
    conn.commit()
    conn.close()


def _update_hold_counter(account_name: str, items: List[Dict[str, Any]]) -> None:
    conn = _ensure_purchase_db(account_name)
    cur = conn.cursor()
    count_new = sum(1 for it in items if int(it.get("hold") or 0) <= 7)
    cur.execute("SELECT value FROM stats WHERE key='hold_le_7'")
    row = cur.fetchone()
    current = float(row["value"]) if row else 0.0
    new_total = current + count_new
    cur.execute("REPLACE INTO stats(key, value) VALUES('hold_le_7', ?)", (new_total,))
    conn.commit()
    conn.close()
    if new_total > 800:
        print(f"[LISS][INFO] hold<=7 для {account_name}: {new_total}, запрашиваем баланс")
        _apply_min_price_limit(account_name)


def _apply_min_price_limit(account_name: str) -> None:
    accounts = _load_liss_accounts()
    acc = next((a for a in accounts if a.name == account_name), None)
    if not acc:
        return
    headers = {"Accept": "application/json", "Authorization": f"Bearer {acc.api_key}"}
    resp = _request_with_retry("GET", BALANCE_URL, headers=headers)
    if resp.status_code not in (200, 201):
        print(f"[LISS][WARN] balance запрос вернул {resp.status_code}, min price не ограничен")
        return
    try:
        balance = float((resp.json().get("data") or {}).get("balance", 0))
    except Exception:
        balance = 0.0
    conn = _ensure_purchase_db(account_name)
    cur = conn.cursor()
    cur.execute("SELECT value FROM stats WHERE key='hold_le_7'")
    row = cur.fetchone()
    hold_value = float(row["value"]) if row else 0.0
    denom = max(1.0, 1000 - hold_value)
    min_price = balance / denom
    cur.execute("REPLACE INTO stats(key, value) VALUES('min_price', ?)", (min_price,))
    conn.commit()
    conn.close()
    print(f"[LISS][INFO] Для аккаунта {account_name} установлен min_price={min_price:.4f}")


def _update_steam_tracking(item_name: str, quantity: int, total_price: float) -> None:
    conn = priceAnalys.get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT purchased_lots, purchased_sum FROM steam_items WHERE item_name = ?",
        (item_name,),
    )
    row = cur.fetchone()
    now = datetime.utcnow()
    if row is None:
        conn.close()
        return
    purchased_lots = float(row["purchased_lots"] or 0) + quantity
    purchased_sum = float(row["purchased_sum"] or 0) + total_price
    priceAnalys.update_purchase_tracking(item_name, purchased_lots, purchased_sum, now, now)
    conn.close()


def _filter_success_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    filtered: List[Dict[str, Any]] = []
    for it in items:
        error_value = it.get("error")
        status = it.get("status")
        if error_value not in (None, 0, "0"):
            print(f"[LISS][WARN] лот {it.get('id')} пропущен из-за error={error_value}")
            continue
        if status in {"wait_unlock", "wait_accept", "accepted", "processing", "wait_withdraw"}:
            filtered.append(it)
        elif status == "return":
            print(
                f"[LISS][WARN] лот {it.get('id')} возвращён, причина: {it.get('return_reason')}"
            )
    return filtered


def _merge_purchase_with_search(
    purchased: List[Dict[str, Any]], search_items: Dict[int, Dict[str, Any]], custom_id: str
) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    for skin in purchased:
        skin_id = int(skin.get("id")) if skin.get("id") is not None else None
        search_info = search_items.get(skin_id, {})
        merged.append(
            {
                "id": skin_id,
                "name": search_info.get("name"),
                "price": skin.get("price", search_info.get("price")),
                "unlock_at": search_info.get("unlock_at"),
                "created_at": search_info.get("created_at"),
                "item_float": search_info.get("item_float"),
                "hold": search_info.get("hold"),
                "stickers": search_info.get("stickers", []),
                "custom_id": custom_id,
                "status": skin.get("status"),
                "return_reason": skin.get("return_reason"),
            }
        )
    return merged


def _handle_buy_response(
    resp: requests.Response,
    ids: Sequence[int],
    error_counter: Dict[Tuple[str, Tuple[int, ...]], int],
    account: "lis_api.LisAccount",
    search_items: Dict[int, Dict[str, Any]],
    custom_id: str,
) -> Optional[PurchaseResult]:
    if resp.status_code in (200, 201):
        data = resp.json().get("data") or {}
        skins = data.get("skins") or []
        valid = _filter_success_items(skins)
        merged = _merge_purchase_with_search(valid, search_items, custom_id)
        _record_purchases(account.name, merged)
        _update_hold_counter(account.name, merged)
        if merged:
            total_price = sum(float(it.get("price") or 0) for it in merged)
            _update_steam_tracking(merged[0].get("name", ""), len(merged), total_price)
        return PurchaseResult(success_items=merged, skipped_items=[], account=account.name, custom_id=custom_id)

    if resp.status_code == 400:
        body = resp.json()
        err = str(body.get("error"))
        key = (err, tuple(sorted(ids)))
        error_counter[key] = error_counter.get(key, 0) + 1
        count = error_counter[key]
        if err == "custom_id_already_exists":
            return None  # сигнализируем о необходимости нового custom_id и повтора
        if err == "skins_unavailable":
            print("[LISS][INFO] Скины недоступны, пропускаем: ids=", ids)
            return PurchaseResult([], [search_items[i] for i in ids if i in search_items], account.name, custom_id)
        if err == "skins_price_higher_than_max_price":
            if count >= 2:
                print(f"[LISS][WARN] Повторная ошибка skins_price_higher_than_max_price для {ids}, пропускаем")
                return PurchaseResult([], [search_items[i] for i in ids if i in search_items], account.name, custom_id)
            available = _check_availability(account, ids)
            if available:
                new_ids = list(available.keys())
                new_sum = sum(available.values()) + 0.02
                new_custom = _generate_custom_id()
                return purchase_lots_with_ids(account, new_ids, new_sum, search_items, new_custom, error_counter)
            return PurchaseResult([], [search_items[i] for i in ids if i in search_items], account.name, custom_id)
        if err in {"invalid_trade_url", "user_trade_ban", "user_cant_trade", "private_inventory", "too_many_failed_attempts_for_user"}:
            raise AccountSkipError(f"{err}")
        return PurchaseResult([], [search_items[i] for i in ids if i in search_items], account.name, custom_id)

    if resp.status_code == 422:
        body = resp.json()
        err = str(body.get("error"))
        key = (err, tuple(sorted(ids)))
        error_counter[key] = error_counter.get(key, 0) + 1
        count = error_counter[key]
        if err in {"invalid_partner_value", "invalid_token_value"}:
            raise AccountSkipError(err)
        if err in {"invalid_ids_value", "invalid_max_price_value"}:
            if count >= 2:
                return PurchaseResult([], [search_items[i] for i in ids if i in search_items], account.name, custom_id)
            available = _check_availability(account, ids)
            if available:
                new_ids = list(available.keys())
                new_sum = sum(available.values()) + 0.02
                new_custom = _generate_custom_id()
                return purchase_lots_with_ids(account, new_ids, new_sum, search_items, new_custom, error_counter)
            return PurchaseResult([], [search_items[i] for i in ids if i in search_items], account.name, custom_id)
        if err == "invalid_custom_id_value":
            if count >= 2:
                return PurchaseResult([], [search_items[i] for i in ids if i in search_items], account.name, custom_id)
            return None
        return PurchaseResult([], [search_items[i] for i in ids if i in search_items], account.name, custom_id)

    return PurchaseResult([], [search_items[i] for i in ids if i in search_items], account.name, custom_id)


def purchase_lots_with_ids(
    account: "lis_api.LisAccount",
    ids: Sequence[int],
    total_price: float,
    search_items: Dict[int, Dict[str, Any]],
    custom_id: Optional[str] = None,
    error_counter: Optional[Dict[Tuple[str, Tuple[int, ...]], int]] = None,
) -> PurchaseResult:
    if error_counter is None:
        error_counter = {}
    if custom_id is None:
        custom_id = _generate_custom_id()
    max_price = float(total_price) + 0.02
    try:
        resp = _send_buy_request(account, ids, max_price, custom_id)
    except requests.exceptions.Timeout:
        info = _get_purchase_info(account, [custom_id])
        if info:
            skins = (info[0].get("skins") if info else []) or []
            valid = _filter_success_items(skins)
            merged = _merge_purchase_with_search(valid, search_items, custom_id)
            _record_purchases(account.name, merged)
            _update_hold_counter(account.name, merged)
            return PurchaseResult(merged, [], account.name, custom_id)
        print("[LISS][WARN] Таймаут запроса и нет данных info, повторяем покупку")
        return purchase_lots_with_ids(account, ids, total_price, search_items, _generate_custom_id(), error_counter)

    result = _handle_buy_response(resp, ids, error_counter, account, search_items, custom_id)
    if result is None:
        # Требуется повтор с новым custom_id
        return purchase_lots_with_ids(account, ids, total_price, search_items, _generate_custom_id(), error_counter)
    return result


def purchase_items(
    search_items: Sequence[Dict[str, Any]],
    account_name: Optional[str] = None,
) -> PurchaseResult:
    if not search_items:
        raise ValueError("Пустой список лотов для покупки")
    accounts = _load_liss_accounts()
    if account_name:
        account = next((a for a in accounts if a.name == account_name), None)
        if account is None:
            raise RuntimeError(f"Аккаунт {account_name} не найден")
    else:
        account = accounts[0]

    ids = [int(it["id"]) for it in search_items if "id" in it]
    total_price = sum(float(it.get("price", 0)) for it in search_items)
    search_map = {int(it["id"]): it for it in search_items if "id" in it}
    try:
        result = purchase_lots_with_ids(account, ids, total_price, search_map)
    except AccountSkipError as exc:
        print(f"[LISS][WARN] Аккаунт {account.name} пропущен: {exc}")
        remaining = [a for a in accounts if a.name != account.name]
        if not remaining:
            raise
        next_account = remaining[0]
        return purchase_lots_with_ids(next_account, ids, total_price, search_map)
    return result

