"""LIS-SKINS API and WebSocket helpers.

This module provides:
- ``LissApiClient`` for REST methods using either ``aiohttp.ClientSession``
  or ``httpx.AsyncClient`` as a transport.
- ``fetch_full_json_for_game`` to download full price lists in JSON format.
- ``LissWebSocketClient`` to consume real-time lot events from the Centrifugo
  backend.

/v1/market/search intentionally is not wrapped here and should be avoided or
used крайне редко: LIS рекомендует опираться на JSON-прайсы и WebSocket,
а новые лоты в search приходят с задержкой.

The logic is written without referencing external documentation URLs; request
and response schemas are embedded below as comments and data-mapping helpers.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import config

try:  # Optional imports: the caller controls which client to pass in
    import aiohttp
except Exception:  # pragma: no cover - optional dependency
    aiohttp = None  # type: ignore

try:
    import httpx
except Exception:  # pragma: no cover - optional dependency
    httpx = None  # type: ignore

try:
    import websockets
except Exception:  # pragma: no cover - optional dependency
    websockets = None  # type: ignore


logger = logging.getLogger("liss_api")


class LissApiClient:
    """Lightweight async client for LIS-SKINS REST API.

    Parameters
    ----------
    api_key:
        Secret token issued for LIS-SKINS account authorization. It is sent in
        the ``Authorization`` header as a bearer token and duplicated in
        ``X-Api-Key`` for compatibility.
    account_name:
        Alias of the account shown in LIS-SKINS UI; forwarded in the
        ``X-Account-Name`` header where the backend expects it.
    session:
        Either :class:`aiohttp.ClientSession` or :class:`httpx.AsyncClient`.
    """

    def __init__(self, api_key: str, account_name: str, session: Any):
        self.api_key = api_key
        self.account_name = account_name
        self.session = session
        self.base_url = config.LISS_API_BASE_URL.rstrip("/")

    def _build_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "X-Api-Key": self.api_key,
            "X-Account-Name": self.account_name,
        }

    @property
    def _is_aiohttp(self) -> bool:
        return aiohttp is not None and isinstance(self.session, aiohttp.ClientSession)

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        url = f"{self.base_url}/{path.lstrip('/')}"
        headers = kwargs.pop("headers", {})
        headers.update(self._build_headers())

        try:
            if self._is_aiohttp:
                async with self.session.request(method, url, headers=headers, **kwargs) as resp:
                    if resp.status >= 400:
                        body = await resp.text()
                        logger.error(
                            "[HTTP] %s %s account=%s status=%s body=%s",
                            method,
                            url,
                            self.account_name,
                            resp.status,
                            body,
                        )
                        if resp.status in {423, 429}:
                            logger.warning(
                                "[HTTP] Возможная блокировка LIS (часто не забираешь скины?) account=%s",
                                self.account_name,
                            )
                        resp.raise_for_status()
                    return await resp.json()

            if httpx is not None and isinstance(self.session, httpx.AsyncClient):
                response = await self.session.request(method, url, headers=headers, **kwargs)
                if response.is_error:
                    body = response.text
                    logger.error(
                        "[HTTP] %s %s account=%s status=%s body=%s",
                        method,
                        url,
                        self.account_name,
                        response.status_code,
                        body,
                    )
                    if response.status_code in {423, 429}:
                        logger.warning(
                            "[HTTP] Возможная блокировка LIS (часто не забираешь скины?) account=%s",
                            self.account_name,
                        )
                    response.raise_for_status()
                return response.json()

            raise TypeError("Unsupported session type; expected aiohttp.ClientSession or httpx.AsyncClient")
        except Exception:
            logger.exception(
                "[HTTP] Ошибка при запросе %s %s для account=%s", method, url, self.account_name
            )
            raise

    async def search_skins(
        self,
        game_code: int,
        steam_market_name: str,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Вызов метода поиска скинов (доступных к покупке).

        Parameters
        ----------
        game_code:
            730 или 570.
        steam_market_name:
            Строка market name (как в Steam).
        limit, offset:
            Параметры пагинации.

        Returns
        -------
        dict
            Raw JSON словарь, который может содержать поля ``items`` / ``skins`` /
            ``results`` с лотами и флаги пагинации ``has_more`` / ``next_offset``.
        """

        payload = {
            "game": game_code,
            "search": steam_market_name,
            "limit": limit,
            "offset": offset,
        }
        return await self._request("POST", "/v1/market/search", json=payload)

    async def get_balance(self) -> float:
        """Return available balance in USD as a float.

        Expected response envelope examples (both are supported):
        ``{"balance": 12.34}`` or ``{"data": {"balance": {"available": 12.34}}}``.
        """

        payload = await self._request("GET", "/account/balance")
        balance_field = payload.get("balance") or payload.get("data", {}).get("balance")

        if isinstance(balance_field, dict):
            raw_value = balance_field.get("available") or balance_field.get("amount") or balance_field.get("value")
        else:
            raw_value = balance_field

        try:
            return float(raw_value)
        except (TypeError, ValueError):
            return 0.0

    async def buy_items(self, items: Iterable[Dict[str, Any]]) -> Any:
        """Purchase items by unique lot identifiers.

        Parameters
        ----------
        items:
            Iterable of item descriptors (dicts). Each item must include one of
            the ID fields used by LIS-SKINS market: ``id``/``lot_id``/``item_id``
            or ``lis_item_id``. Duplicates are removed before sending.

        The request body is ``{"items": [...], "skip_unavailable": true}`` so
        already-sold lots do not block batch purchases. The raw API response is
        returned to the caller, typically containing per-lot results with
        ``purchase_id`` and ``item_asset_id`` keys.
        """

        unique_items: List[Dict[str, Any]] = []
        seen_ids = set()

        for item in items:
            lot_id = (
                item.get("lot_id")
                or item.get("lis_item_id")
                or item.get("item_id")
                or item.get("id")
                or item.get("custom_id")
            )
            if lot_id is None or lot_id in seen_ids:
                continue
            seen_ids.add(lot_id)
            unique_items.append(item)

        body = {"items": unique_items, "skip_unavailable": True}
        return await self._request("POST", "/market/buy", json=body)

    async def get_purchase_info(self, purchase_id: str) -> Any:
        """Fetch purchase status and item details by purchase identifier.

        Tries the dedicated ``/market/info`` endpoint first; if the backend
        returns an empty payload, the fallback is ``/market/history`` with a
        single record filter. The returned structure is not transformed so the
        caller can inspect fields such as ``item_asset_id``, ``status`` and
        unlock timestamp.
        """

        info = await self._request("GET", "/market/info", params={"purchase_id": purchase_id})
        if info:
            return info

        history = await self._request(
            "GET", "/market/history", params={"purchase_id": purchase_id, "limit": 1}
        )
        return history

    async def withdraw_items(
        self,
        purchase_ids: Optional[Iterable[str]] = None,
        custom_ids: Optional[Iterable[str]] = None,
        partner: Optional[str] = None,
        token: Optional[str] = None,
    ) -> Any:
        """Withdraw specific items using ``/market/withdraw``.

        Args mirror the API contract: one of ``purchase_ids`` or ``custom_ids``
        must be provided; ``partner`` and ``token`` correspond to Steam trade
        URL components.
        """

        body: Dict[str, Any] = {}
        if purchase_ids:
            body["purchase_ids"] = list(purchase_ids)
        if custom_ids:
            body["custom_ids"] = list(custom_ids)
        if partner:
            body["partner"] = partner
        if token:
            body["token"] = token

        return await self._request("POST", "/market/withdraw", json=body)

    async def withdraw_all(self, partner: Optional[str] = None, token: Optional[str] = None) -> Any:
        """Withdraw all eligible items with one call to ``/market/withdraw-all``."""

        body: Dict[str, Any] = {}
        if partner:
            body["partner"] = partner
        if token:
            body["token"] = token

        return await self._request("POST", "/market/withdraw-all", json=body)


def _save_full_json_snapshot(raw_items: List[Dict[str, Any]], game_code: int) -> None:
    snapshot_dir = Path(config.LISS_JSON_SNAPSHOT_DIR)
    try:
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        game_label = {730: "csgo", 570: "dota2"}.get(game_code, str(game_code))
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        snapshot_path = snapshot_dir / f"{game_label}_full_{timestamp}.json"

        with snapshot_path.open("w", encoding="utf-8") as f:
            json.dump(raw_items, f, ensure_ascii=False)

        logger.info("[JSON] Сохранили снапшот %s (%s лотов)", snapshot_path, len(raw_items))
    except Exception:
        logger.exception(
            "[JSON] Не удалось сохранить JSON снапшот для game_code=%s", game_code
        )


async def fetch_full_json_for_game(game_code: int, session: Optional[Any] = None) -> List[Dict[str, Any]]:
    """Download full LIS-SKINS JSON price list for a game.

    Parameters
    ----------
    game_code:
        730 for Counter-Strike 2, 570 for Dota 2.
    session:
        Optional ``aiohttp.ClientSession`` or ``httpx.AsyncClient``. If omitted,
        a temporary :class:`httpx.AsyncClient` is created.

    Returns
    -------
    list of dict
        Raw list of entries returned by LIS, without additional normalization.
    """

    file_map = {730: "csgo.json", 570: "dota2.json"}
    if game_code not in file_map:
        raise ValueError(f"Unsupported game code: {game_code}")

    url = f"{config.LISS_JSON_BASE_URL.rstrip('/')}/{file_map[game_code]}"

    async def _download(client: Any) -> Any:
        headers = {"User-Agent": "liss-client/1.0"}
        if aiohttp is not None and isinstance(client, aiohttp.ClientSession):
            async with client.get(url, headers=headers) as resp:
                resp.raise_for_status()
                return await resp.json()
        if httpx is not None and isinstance(client, httpx.AsyncClient):
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            return resp.json()
        raise TypeError("Unsupported session type; expected aiohttp.ClientSession or httpx.AsyncClient")

    if session is None:
        if httpx is None:
            raise RuntimeError("httpx is required when session is not provided")
        async with httpx.AsyncClient() as client:
            raw = await _download(client)
    else:
        raw = await _download(session)

    if isinstance(raw, dict):
        raw_items = raw.get("items") or []
    elif isinstance(raw, list):
        raw_items = raw
    else:
        logger.error("[JSON] Неожиданный формат JSON: %r", type(raw))
        return []

    _save_full_json_snapshot(raw_items, game_code)

    normalized: List[Dict[str, Any]] = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        price_value = item.get("price")
        if name is None or price_value is None:
            continue

        try:
            price_usd = float(price_value)
        except (TypeError, ValueError):
            continue

        try:
            count = int(item.get("count") or 0)
        except (TypeError, ValueError):
            count = 0

        normalized.append(
            {
                "name": name,
                "steam_market_name": name,
                "game_code": game_code,
                "price_usd": price_usd,
                "count": count,
                "raw": item,
            }
        )

    return normalized


class LissWebSocketClient:
    """Centrifugo-based WebSocket consumer for LIS-SKINS events."""

    def __init__(self, api_key: str, queue: asyncio.Queue, url: str = config.LISS_WS_URL):
        if websockets is None:
            raise RuntimeError("websockets package is required for LissWebSocketClient")
        self.api_key = api_key
        self.url = url
        self.queue = queue
        self._msg_id = 0
        self._ws = None

    async def _send(self, payload: Dict[str, Any]) -> None:
        self._msg_id += 1
        payload.setdefault("id", self._msg_id)
        await self._ws.send(json.dumps(payload))

    async def _handle_message(self, message: Dict[str, Any]) -> None:
        if message.get("method") != "message":
            return

        params = message.get("params", {})
        data = params.get("data") or {}
        event_type = data.get("event")
        payload = data.get("payload") or {}

        if event_type not in {
            "obtained_skin_added",
            "obtained_skin_deleted",
            "obtained_skin_price_changed",
            "market_lot_added",
            "market_lot_price_changed",
            "market_lot_deleted",
        }:
            return

        normalized = {
            "event": event_type,
            "lis_item_id": payload.get("id") or payload.get("lis_item_id") or payload.get("lot_id"),
            "steam_market_name": payload.get("market_hash_name") or payload.get("name"),
            "game_code": payload.get("app_id") or payload.get("game"),
            "price_usd": payload.get("price_usd") or payload.get("price"),
            "hold_days": payload.get("hold") or payload.get("hold_days"),
            "is_new_lowest": bool(payload.get("is_lowest_price")),
            "full_raw": payload,
        }
        try:
            normalized["received_ts"] = asyncio.get_running_loop().time()
        except RuntimeError:
            normalized["received_ts"] = 0.0
        await self.queue.put(normalized)

    async def _subscribe(self, channels: Iterable[str]) -> None:
        for channel in channels:
            await self._send({"method": "subscribe", "params": {"channel": channel}})

    async def run(self, channels: Optional[Iterable[str]] = None) -> None:
        """Connect, authenticate and stream lot events into the provided queue."""

        if channels is None:
            channels = [
                "public:obtained_skin_added",
                "public:obtained_skin_deleted",
                "public:obtained_skin_price_changed",
                "public:market_lot_added",
                "public:market_lot_price_changed",
                "public:market_lot_deleted",
            ]

        async with websockets.connect(self.url) as ws:
            self._ws = ws
            await self._send({"method": "connect", "params": {"token": self.api_key}})
            await self._subscribe(channels)

            async for raw in ws:  # type: ignore[attr-defined]
                try:
                    message = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                await self._handle_message(message)
