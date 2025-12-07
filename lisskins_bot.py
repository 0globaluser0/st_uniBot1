"""Асинхронный бот для закупок на Lisskins.

Основные требования описаны в пользовательском сценарии:
- получение списка предметов через JSON и отслеживание обновлений по websocket;
- фильтры по игре, ключевым словам и цене;
- проверка актуальных рекомендованных цен из локальной БД Steam-анализатора;
- при необходимости запуск анализа графика продаж (отдельная очередь);
- покупка подходящих лотов (отдельная очередь) с учётом лимитов и холда;
- учёт блэклиста и хранения данных о покупках по каждому аккаунту.

Файл предоставляет класс :class:`LisskinsBot`, который координирует очереди
обработки и хранение статистики в SQLite, опираясь на реальные REST/WebSocket
запросы публичного API https://lis-skins-ru.stoplight.io/docs/lis-skins-ru-public-user-api.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Deque, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import quote

import config
import priceAnalys
import requests


# ---------------------- Models ----------------------


@dataclass
class LisskinsLot:
    """Описание лота на Lisskins."""

    id: str
    name: str
    game: str
    price_usd: float
    hold_days: int
    amount: int = 1


@dataclass
class CachedPrice:
    rec_price: float
    avg_sales_week: float
    updated_at: datetime
    tier: int
    graph_type: str


@dataclass
class PurchaseSummary:
    item_name: str
    total_count: int
    total_spent: float
    first_purchase_at: datetime
    spend_period_days: int
    count_period_days: int


# ---------------------- Storage helpers ----------------------


class PurchaseStorage:
    """Управляет базой покупок для конкретного аккаунта Lisskins."""

    def __init__(self, account_name: str) -> None:
        db_name = config.PURCHASES_DB_TEMPLATE.format(account=account_name)
        self.db_path = os.path.abspath(db_name)
        self._ensure_db()

    def _ensure_db(self) -> None:
        import sqlite3

        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS purchases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_name TEXT,
                lot_id TEXT,
                price_usd REAL,
                rec_price REAL,
                amount INTEGER,
                purchased_at TEXT,
                unlock_at TEXT,
                account TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS purchase_summary (
                item_name TEXT PRIMARY KEY,
                total_count INTEGER,
                total_spent REAL,
                first_purchase_at TEXT
            )
            """
        )
        conn.commit()
        conn.close()

    # region queries
    def record_purchase(
        self,
        lot: LisskinsLot,
        rec_price: float,
        account: str,
        unlock_at: datetime,
    ) -> None:
        import sqlite3

        now = datetime.utcnow()
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO purchases(item_name, lot_id, price_usd, rec_price, amount, purchased_at, unlock_at, account)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                lot.name,
                lot.id,
                lot.price_usd,
                rec_price,
                lot.amount,
                now.isoformat(timespec="seconds"),
                unlock_at.isoformat(timespec="seconds"),
                account,
            ),
        )

        cur.execute(
            """
            INSERT INTO purchase_summary(item_name, total_count, total_spent, first_purchase_at)
            VALUES(?,?,?,?)
            ON CONFLICT(item_name) DO UPDATE SET
                total_count = total_count + excluded.total_count,
                total_spent  = total_spent  + excluded.total_spent
            """,
            (
                lot.name,
                lot.amount,
                lot.price_usd * lot.amount,
                now.isoformat(timespec="seconds"),
            ),
        )

        conn.commit()
        conn.close()

    def get_summary(self, item_name: str) -> Optional[PurchaseSummary]:
        import sqlite3

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM purchase_summary WHERE item_name = ?",
            (item_name,),
        )
        row = cur.fetchone()
        conn.close()
        if not row:
            return None
        return PurchaseSummary(
            item_name=row["item_name"],
            total_count=row["total_count"],
            total_spent=row["total_spent"],
            first_purchase_at=datetime.fromisoformat(row["first_purchase_at"]),
            spend_period_days=config.SPEND_LIMIT_DAYS,
            count_period_days=config.PURCHASE_LIMIT_DAYS,
        )

    def count_hold_items(self, max_hold_days: int = 7) -> int:
        import sqlite3

        boundary = datetime.utcnow() + timedelta(days=max_hold_days)
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COALESCE(SUM(amount), 0) FROM purchases
            WHERE unlock_at <= ?
            """,
            (boundary.isoformat(timespec="seconds"),),
        )
        value = cur.fetchone()[0]
        conn.close()
        return int(value or 0)

    def count_spent_in_period(self, item_name: str, days: int) -> float:
        import sqlite3

        boundary = datetime.utcnow() - timedelta(days=days)
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COALESCE(SUM(price_usd * amount), 0)
            FROM purchases
            WHERE item_name = ? AND purchased_at >= ?
            """,
            (item_name, boundary.isoformat(timespec="seconds")),
        )
        spent = cur.fetchone()[0]
        conn.close()
        return float(spent or 0.0)

    def count_purchased_in_period(self, item_name: str, days: int) -> int:
        import sqlite3

        boundary = datetime.utcnow() - timedelta(days=days)
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COALESCE(SUM(amount), 0)
            FROM purchases
            WHERE item_name = ? AND purchased_at >= ?
            """,
            (item_name, boundary.isoformat(timespec="seconds")),
        )
        count = cur.fetchone()[0]
        conn.close()
        return int(count or 0)

    # endregion


# ---------------------- Lisskins client stubs ----------------------


class LisskinsClient:
    """Заглушка клиента для запросов к Lisskins.

    Методы используют requests для совместимости, но реальное построение URL и
    авторизация должны быть реализованы по документации API.
    """

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    def fetch_balance(self) -> Optional[float]:
        """Возвращает доступный баланс аккаунта через публичное API."""

        url = f"{config.LISSKINS_API_URL}/user/balance"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        try:
            response = requests.get(url, headers=headers, timeout=config.HTTP_TIMEOUT)
            response.raise_for_status()
        except Exception as exc:  # pragma: no cover - сетевые ошибки
            print(f"[LISSKINS] Не удалось получить баланс: {exc}")
            return None

        data: Any = None
        try:
            data = response.json()
        except ValueError:
            print("[LISSKINS] Некорректный ответ при получении баланса")
            return None

        if isinstance(data, dict):
            for key in ("balance", "available_balance", "available", "availableBalance"):
                value = data.get(key)
                try:
                    if value is not None:
                        return float(value)
                except (TypeError, ValueError):
                    continue
        return None

    def fetch_items_json(self) -> List[Dict[str, Any]]:
        url = f"{config.LISSKINS_API_URL}/market/list"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        try:
            response = requests.get(url, headers=headers, timeout=config.HTTP_TIMEOUT)
            response.raise_for_status()
        except Exception as exc:  # pragma: no cover - сетевые ошибки
            print(f"[LISSKINS] Не удалось получить список предметов: {exc}")
            return []

        try:
            data = response.json()
        except ValueError:
            print("[LISSKINS] Некорректный JSON при запросе списка предметов")
            return []

        if isinstance(data, dict):
            items = data.get("items") or data.get("data") or data.get("list")
            if isinstance(items, list):
                return [item for item in items if isinstance(item, dict)]
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        return []

    def buy_lots(self, lot_ids: List[str]) -> bool:
        if not lot_ids:
            return False

        url = f"{config.LISSKINS_API_URL}/market/buy"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {"ids": lot_ids, "lot_ids": lot_ids}
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=config.HTTP_TIMEOUT)
            response.raise_for_status()
        except Exception as exc:  # pragma: no cover - сетевые ошибки
            print(f"[LISSKINS] Ошибка покупки {lot_ids}: {exc}")
            return False

        try:
            data = response.json()
        except ValueError:
            print("[LISSKINS] Некорректный ответ при покупке")
            return False

        if isinstance(data, dict):
            if data.get("success") is True:
                return True
            status = str(data.get("status") or data.get("result") or "").lower()
            if status in {"ok", "success", "purchased"}:
                return True
            if data.get("error") or data.get("errors"):
                print(f"[LISSKINS] Ошибка ответа при покупке {lot_ids}: {data}")
                return False

        print(f"[LISSKINS] Не удалось подтвердить покупку {lot_ids}: {data}")
        return False


# ---------------------- Bot logic ----------------------


class LisskinsBot:
    def __init__(self, api_key: str, account_name: str) -> None:
        self.api_key = api_key
        self.account_name = account_name
        self.client = LisskinsClient(api_key)
        self.purchase_storage = PurchaseStorage(account_name)

        # Анализируемые лоты идут через PriorityQueue, чтобы свежие события из
        # websocket могли обгонять обычный разбор JSON.
        self.analysis_queue: asyncio.PriorityQueue[Tuple[int, float, LisskinsLot]] = (
            asyncio.PriorityQueue()
        )
        self.purchase_queue: asyncio.Queue[LisskinsLot] = asyncio.Queue()

        self.websocket_priority: Deque[Tuple[float, LisskinsLot]] = deque()
        self.market: Dict[str, List[LisskinsLot]] = defaultdict(list)
        self._known_lot_ids: Set[str] = set()
        self._analysis_inflight: Set[str] = set()
        self._item_locks: Dict[str, asyncio.Lock] = {}
        # (timestamp, balance)
        self._balance_cache: Optional[Tuple[float, float]] = None

    # region Filters and utilities
    def _game_allowed(self, lot: LisskinsLot) -> bool:
        game = lot.game.lower()
        if "cs" in game:
            return config.ALLOW_CS2
        if "dota" in game:
            return config.ALLOW_DOTA2
        return False

    def _has_excluded_keyword(self, name: str) -> bool:
        lowered = name.lower()
        return any(keyword.lower() in lowered for keyword in config.EXCLUDED_KEYWORDS)

    def _price_in_range(self, price: float) -> bool:
        return config.LISSKINS_PRICE_MIN <= price <= config.LISSKINS_PRICE_MAX

    def _profitability(self, price: float, rec_price: float) -> float:
        if price <= 0:
            return -1.0
        return (rec_price * 0.8697 / price) - 1.0

    def _steam_url_for_lot(self, lot: LisskinsLot) -> Optional[str]:
        game = lot.game.lower()
        if "cs" in game:
            game_code = 730
        elif "dota" in game:
            game_code = 570
        else:
            return None
        return f"https://steamcommunity.com/market/listings/{game_code}/{quote(lot.name)}"

    def _has_actual_cached_price(self, lot: LisskinsLot) -> Optional[CachedPrice]:
        cached = priceAnalys.get_cached_item(lot.name)
        if not cached:
            return None
        updated_at = datetime.fromisoformat(cached["updated_at"])
        if datetime.utcnow() - updated_at > timedelta(hours=config.ACTUAL_HOURS):
            return None
        return CachedPrice(
            rec_price=cached["rec_price"],
            avg_sales_week=cached["avg_sales"],
            updated_at=updated_at,
            tier=cached["tier"],
            graph_type=cached["graph_type"],
        )

    def _passes_filters(self, lot: LisskinsLot) -> bool:
        if not self._game_allowed(lot):
            return False
        if self._has_excluded_keyword(lot.name):
            return False
        if not self._price_in_range(lot.price_usd):
            return False
        if lot.hold_days > config.MAX_HOLD_DAYS:
            return False

        bl_entry = priceAnalys.get_blacklist_entry(lot.name)
        if bl_entry:
            expires = datetime.fromisoformat(bl_entry["expires_at"])
            if expires > datetime.utcnow():
                return False
            priceAnalys.remove_from_blacklist(lot.name)
        return True

    async def _balance_hint(self) -> Optional[float]:
        now = time.time()
        if self._balance_cache and now - self._balance_cache[0] < 60:
            return self._balance_cache[1]

        loop = asyncio.get_running_loop()
        balance = await loop.run_in_executor(None, self.client.fetch_balance)
        if balance is not None:
            self._balance_cache = (now, balance)
        return balance

    async def _respect_inventory_limit(self, price: float) -> bool:
        current_hold = self.purchase_storage.count_hold_items(max_hold_days=7)
        if current_hold < 800:
            return True
        slots_left = max(0, 1000 - current_hold)
        if slots_left <= 0:
            return False

        balance_hint = await self._balance_hint()
        min_price = config.LISSKINS_PRICE_MIN
        if isinstance(balance_hint, (int, float)) and balance_hint > 0:
            min_price = max(min_price, balance_hint / slots_left)
        return price >= min_price

    # endregion

    # region queueing
    async def enqueue_initial_items(self) -> None:
        items = self.client.fetch_items_json()
        total = len(items)
        print(f"[JSON] Найдено {total} предметов для сканирования")
        for idx, data in enumerate(items, start=1):
            lot = self._lot_from_json(data)
            self._register_lot(lot)
            await self._process_market_for_item(lot.name)
            print(f"[JSON] Обработано {idx}/{total}")

    def _register_lot(self, lot: LisskinsLot) -> None:
        if lot.id in self._known_lot_ids:
            return
        if not self._passes_filters(lot):
            return
        self._known_lot_ids.add(lot.id)
        self.market[lot.name].append(lot)

    def _cleanup_item(self, item_name: str) -> None:
        lots = [lot for lot in self.market.get(item_name, []) if lot.id in self._known_lot_ids]
        self.market[item_name] = lots
        if not lots:
            self.market.pop(item_name, None)

    def _mark_purchased(self, lot: LisskinsLot) -> None:
        if lot.id in self._known_lot_ids:
            self._known_lot_ids.remove(lot.id)
        self.market[lot.name] = [l for l in self.market.get(lot.name, []) if l.id != lot.id]
        if not self.market.get(lot.name):
            self.market.pop(lot.name, None)

    async def _process_market_for_item(self, item_name: str, priority: bool = False) -> None:
        lock = self._item_locks.setdefault(item_name, asyncio.Lock())
        async with lock:
            lots = [lot for lot in self.market.get(item_name, []) if self._passes_filters(lot)]
            if not lots:
                return

            cached = self._has_actual_cached_price(lots[0])
            if not cached:
                await self._schedule_analysis(item_name, lots, priority=priority)
                return

            await self._enqueue_purchases_for_item(item_name, lots, cached)

    async def _schedule_analysis(
        self, item_name: str, lots: List[LisskinsLot], *, priority: bool = False
    ) -> None:
        if item_name in self._analysis_inflight:
            return
        lot = min(lots, key=lambda l: (l.price_usd, l.hold_days))
        self._analysis_inflight.add(item_name)
        prio_value = 0 if priority else 1
        await self.analysis_queue.put((prio_value, time.time(), lot))

    async def _enqueue_purchases_for_item(
        self, item_name: str, lots: List[LisskinsLot], cached: CachedPrice
    ) -> None:
        max_allowed = cached.avg_sales_week * config.MAX_LOTS_PERCENT_OF_AVG_SALES
        already_purchased = self.purchase_storage.count_purchased_in_period(
            item_name, config.PURCHASE_LIMIT_DAYS
        )
        already_spent = self.purchase_storage.count_spent_in_period(
            item_name, config.SPEND_LIMIT_DAYS
        )

        remaining_count = max(0, int(max_allowed - already_purchased))
        remaining_spend = max(0.0, config.MAX_SPEND_USD_PER_PERIOD - already_spent)
        if remaining_count <= 0 or remaining_spend <= 0:
            return

        selected: List[LisskinsLot] = []
        total_count = 0
        total_spend = 0.0

        for lot in sorted(lots, key=lambda l: (l.price_usd, l.hold_days)):
            profitability = self._profitability(lot.price_usd, cached.rec_price)
            if profitability < config.MIN_PROFITABILITY:
                continue
            if total_count + lot.amount > remaining_count:
                continue
            projected_spend = total_spend + lot.price_usd * lot.amount
            if projected_spend > remaining_spend:
                continue
            if not await self._respect_inventory_limit(lot.price_usd):
                continue

            selected.append(lot)
            total_count += lot.amount
            total_spend = projected_spend

        for lot in selected:
            await self.purchase_queue.put(lot)

    def _lot_from_json(self, data: Dict[str, Any]) -> LisskinsLot:
        return LisskinsLot(
            id=str(data.get("id")),
            name=str(data.get("name")),
            game=str(data.get("game", "")),
            price_usd=float(data.get("price", 0.0)),
            hold_days=int(data.get("hold_days", 0)),
            amount=int(data.get("amount", 1)),
        )

    def add_websocket_lot(self, data: Dict[str, Any]) -> None:
        lot = self._lot_from_json(data)
        now = time.time()
        # Лоты из websocket встает в начало очереди в течение окна приоритета.
        self.websocket_priority.append((now, lot))

    async def pump_websocket_priority(self) -> None:
        while True:
            if not self.websocket_priority:
                await asyncio.sleep(0.2)
                continue
            ts, lot = self.websocket_priority.popleft()
            age = time.time() - ts
            if age <= config.WEBSOCKET_PRIORITY_WINDOW:
                self._register_lot(lot)
                await self._process_market_for_item(lot.name, priority=True)
            else:
                self._register_lot(lot)
                await self._process_market_for_item(lot.name)

    async def websocket_listener(self) -> None:
        if not config.LISSKINS_WS_URL:
            return
        try:
            import websockets  # type: ignore
        except ImportError:
            print("[WARN] Модуль websockets не установлен, поток обновлений отключён")
            return

        headers = {"Authorization": f"Bearer {self.api_key}"}
        while True:
            try:
                async with websockets.connect(
                    config.LISSKINS_WS_URL, extra_headers=headers, open_timeout=config.HTTP_TIMEOUT
                ) as ws:
                    async for raw in ws:
                        try:
                            payload = json.loads(raw)
                        except Exception:
                            continue
                        if isinstance(payload, dict):
                            lot_data = payload.get("data") or payload
                            if isinstance(lot_data, dict):
                                self.add_websocket_lot(lot_data)
            except Exception as exc:  # pragma: no cover - сеть/отключение
                print(f"[WARN] WebSocket разорван: {exc}, переподключение через 2с")
                await asyncio.sleep(2)

    # endregion

    # region workers
    async def analysis_worker(self) -> None:
        while True:
            prio, _, lot = await self.analysis_queue.get()
            try:
                await self._analyse_and_enqueue(lot)
            finally:
                self.analysis_queue.task_done()

    async def purchase_worker(self) -> None:
        while True:
            lot = await self.purchase_queue.get()
            try:
                await self._attempt_purchase(lot)
            finally:
                self.purchase_queue.task_done()

    async def _analyse_and_enqueue(self, lot: LisskinsLot) -> None:
        url = self._steam_url_for_lot(lot)
        if not url:
            print(f"[ANALYSIS] {lot.name}: неизвестная игра, пропуск")
            self._analysis_inflight.discard(lot.name)
            return

        print(f"[ANALYSIS] {lot.name}: парсинг {url}")
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, priceAnalys.parsing_steam_sales, url)
        status = result.get("status")
        if status != "ok":
            reason = result.get("message") or status
            print(f"[ANALYSIS] {lot.name}: не удалось рассчитать цену ({reason})")
            self._analysis_inflight.discard(lot.name)
            return

        self._analysis_inflight.discard(lot.name)
        await self._process_market_for_item(lot.name)

    async def _attempt_purchase(self, lot: LisskinsLot) -> None:
        cached = self._has_actual_cached_price(lot)
        if not cached:
            # Срок годности истёк: перенаправляем на анализ.
            self._analysis_inflight.add(lot.name)
            await self.analysis_queue.put((0, time.time(), lot))
            return

        profitability = self._profitability(lot.price_usd, cached.rec_price)
        if profitability < config.MIN_PROFITABILITY:
            return

        # Проверяем ограничения по количеству и сумме за периоды.
        max_allowed = cached.avg_sales_week * config.MAX_LOTS_PERCENT_OF_AVG_SALES
        purchased_count = self.purchase_storage.count_purchased_in_period(
            lot.name, config.PURCHASE_LIMIT_DAYS
        )
        if purchased_count + lot.amount > max_allowed:
            return

        spent = self.purchase_storage.count_spent_in_period(
            lot.name, config.SPEND_LIMIT_DAYS
        )
        if spent + lot.price_usd * lot.amount > config.MAX_SPEND_USD_PER_PERIOD:
            return

        if not await self._respect_inventory_limit(lot.price_usd):
            return

        unlock_at = datetime.utcnow() + timedelta(days=lot.hold_days)
        success = self.client.buy_lots([lot.id])
        if success:
            self.purchase_storage.record_purchase(
                lot=lot, rec_price=cached.rec_price, account=self.account_name, unlock_at=unlock_at
            )
            self._mark_purchased(lot)
            print(
                f"[PURCHASED] {lot.name}: {lot.amount} шт по {lot.price_usd} USD, "
                f"rec_price={cached.rec_price}, hold={lot.hold_days}д"
            )

    # endregion

    async def _run_workers(self) -> None:
        analysis_workers = [asyncio.create_task(self.analysis_worker()) for _ in range(config.ANALYSIS_WORKERS)]
        purchase_workers = [asyncio.create_task(self.purchase_worker()) for _ in range(config.PURCHASE_WORKERS)]
        ws_pump = asyncio.create_task(self.pump_websocket_priority())
        ws_listener = asyncio.create_task(self.websocket_listener())
        await asyncio.gather(*analysis_workers, *purchase_workers, ws_pump, ws_listener)

    async def run(self) -> None:
        await self.enqueue_initial_items()
        await self._run_workers()


# ---------------------- Account helpers ----------------------


def load_accounts() -> List[Dict[str, str]]:
    accounts: List[Dict[str, str]] = []
    if not os.path.exists("liss_accs.txt"):
        print("[WARN] Файл liss_accs.txt не найден, список аккаунтов пуст.")
        return accounts
    with open("liss_accs.txt", "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or ":" not in line:
                continue
            api_key, name = line.split(":", 1)
            accounts.append({"api_key": api_key.strip(), "name": name.strip()})
    return accounts


async def run_accounts_sequentially() -> None:
    accounts = load_accounts()
    if not accounts:
        print("[WARN] Нет аккаунтов для запуска Lisskins-бота.")
        return

    for account in accounts:
        print(f"[ACCOUNT] Запуск закупки для {account['name']}")
        bot = LisskinsBot(api_key=account["api_key"], account_name=account["name"])
        try:
            await bot.run()
        except KeyboardInterrupt:
            print("[ACCOUNT] Остановка по Ctrl+C")
            break


__all__ = [
    "LisskinsBot",
    "LisskinsLot",
    "load_accounts",
    "run_accounts_sequentially",
]
