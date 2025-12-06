"""Асинхронный бот для закупок на Lisskins.

Основные требования описаны в пользовательском сценарии:
- получение списка предметов через JSON и отслеживание обновлений по websocket;
- фильтры по игре, ключевым словам и цене;
- проверка актуальных рекомендованных цен из локальной БД Steam-анализатора;
- при необходимости запуск анализа графика продаж (отдельная очередь);
- покупка подходящих лотов (отдельная очередь) с учётом лимитов и холда;
- учёт блэклиста и хранения данных о покупках по каждому аккаунту.

Файл предоставляет класс :class:`LisskinsBot`, который координирует очереди
обработки и хранение статистики в SQLite. Реальные HTTP/WebSocket запросы
оставлены в виде заглушек: бот можно доработать, подключив настоящие эндпоинты
из документации https://lis-skins-ru.stoplight.io/docs/lis-skins-ru-public-user-api.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Deque, Dict, List, Optional, Tuple

import config
import priceAnalys


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

    def fetch_items_json(self) -> List[Dict[str, Any]]:
        # TODO: заменить на настоящий вызов. Заглушка возвращает пустой список.
        return []

    def buy_lots(self, lot_ids: List[str]) -> bool:
        # TODO: реализовать вызов покупки через API Lisskins. Сейчас просто лог.
        print(f"[LISSKINS] Покупка лотов {lot_ids} (заглушка)")
        return True


# ---------------------- Bot logic ----------------------


class LisskinsBot:
    def __init__(self, api_key: str, account_name: str) -> None:
        self.api_key = api_key
        self.account_name = account_name
        self.client = LisskinsClient(api_key)
        self.purchase_storage = PurchaseStorage(account_name)

        self.analysis_queue: asyncio.Queue[LisskinsLot] = asyncio.Queue()
        self.purchase_queue: asyncio.Queue[LisskinsLot] = asyncio.Queue()

        self.websocket_priority: Deque[Tuple[float, LisskinsLot]] = deque()

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

    def _respect_inventory_limit(self, price: float) -> bool:
        current_hold = self.purchase_storage.count_hold_items(max_hold_days=7)
        if current_hold < 800:
            return True
        slots_left = max(0, 1000 - current_hold)
        if slots_left <= 0:
            return False

        balance_hint = getattr(config, "LISSKINS_BALANCE_USD", None)
        min_price = config.LISSKINS_PRICE_MIN
        if isinstance(balance_hint, (int, float)) and balance_hint > 0:
            min_price = max(min_price, balance_hint / slots_left)
        return price >= min_price

    # endregion

    # region queueing
    async def enqueue_initial_items(self) -> None:
        items = self.client.fetch_items_json()
        for data in items:
            lot = self._lot_from_json(data)
            await self._route_lot(lot)

    async def _route_lot(self, lot: LisskinsLot) -> None:
        if not self._passes_filters(lot):
            return
        cached = self._has_actual_cached_price(lot)
        if cached:
            await self.purchase_queue.put(lot)
        else:
            await self.analysis_queue.put(lot)

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
        if not self._passes_filters(lot):
            return
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
                await self.analysis_queue.put(lot)
            else:
                await self._route_lot(lot)

    # endregion

    # region workers
    async def analysis_worker(self) -> None:
        while True:
            lot = await self.analysis_queue.get()
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
        # Заглушка анализа: здесь должен быть вызов парсинга графика Steam.
        print(f"[ANALYSIS] {lot.name}: требуется парсинг графика (заглушка)")
        # Имитация задержки распределённого парсинга.
        await asyncio.sleep(0)
        # После анализа нужно сохранить rec_price в БД Steam-анализатора.
        # Здесь мы только проверяем, что запись появилась и актуальна.
        cached = self._has_actual_cached_price(lot)
        if cached:
            await self.purchase_queue.put(lot)

    async def _attempt_purchase(self, lot: LisskinsLot) -> None:
        cached = self._has_actual_cached_price(lot)
        if not cached:
            # Срок годности истёк: перенаправляем на анализ.
            await self.analysis_queue.put(lot)
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

        if not self._respect_inventory_limit(lot.price_usd):
            return

        unlock_at = datetime.utcnow() + timedelta(days=lot.hold_days)
        success = self.client.buy_lots([lot.id])
        if success:
            self.purchase_storage.record_purchase(
                lot=lot, rec_price=cached.rec_price, account=self.account_name, unlock_at=unlock_at
            )
            print(
                f"[PURCHASED] {lot.name}: {lot.amount} шт по {lot.price_usd} USD, "
                f"rec_price={cached.rec_price}, hold={lot.hold_days}д"
            )

    # endregion

    async def _run_workers(self) -> None:
        analysis_workers = [asyncio.create_task(self.analysis_worker()) for _ in range(config.ANALYSIS_WORKERS)]
        purchase_workers = [asyncio.create_task(self.purchase_worker()) for _ in range(config.PURCHASE_WORKERS)]
        ws_pump = asyncio.create_task(self.pump_websocket_priority())
        await asyncio.gather(*analysis_workers, *purchase_workers, ws_pump)

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
