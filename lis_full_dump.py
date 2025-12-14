
"""
Загрузка и кэширование полного дампа LIS-SKINS (api_csgo_full.json).

Задача:
- Фоново скачать большой JSON (1M+ лотов) по URL
- Построить локальный SQLite-кэш с индексом по name
- Безопасно переключать активный кэш (A/B), чтобы чтение не мешало записи

Примечание:
- Парсер дампа потоковый (без ijson), работает на байтовом уровне:
  ищет массив "items": [...] и выделяет объекты {...} по балансировке скобок
  с учётом строк и экранирования.
"""

from __future__ import annotations

import json
import os
import threading
import time
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional, Tuple

import orjson
import requests


FULL_DUMP_URL = "https://lis-skins.com/market_export_json/api_csgo_full.json"


@dataclass(frozen=True)
class FullDumpPaths:
    json_a: Path
    json_b: Path
    db_a: Path
    db_b: Path
    state: Path  # json with {"active":"A"|"B", "last_success": int unix, "last_update": int|None}


def _safe_mkdir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _atomic_write_text(path: Path, text: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def _read_state(state_path: Path) -> dict:
    if not state_path.exists():
        return {"active": None, "last_success": 0, "last_update": None}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return {"active": None, "last_success": 0, "last_update": None}


def _write_state(state_path: Path, active: str, last_success: int, last_update: Optional[int]) -> None:
    _atomic_write_text(
        state_path,
        json.dumps({"active": active, "last_success": int(last_success), "last_update": last_update}, ensure_ascii=False),
    )


def _download_with_retry(url: str, dest: Path, *, timeout: int = 120) -> Tuple[int, int]:
    """
    Скачивает файл в dest (перезаписывает).
    Возвращает (bytes_written, last_update_timestamp_or_0_if_unknown).
    Обрабатывает 429: ждём 2 сек и повторяем.
    """
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, br",
        "User-Agent": "liss-bot/1.0",
    }

    while True:
        try:
            with requests.get(url, headers=headers, stream=True, timeout=timeout) as r:
                if r.status_code == 429:
                    print("[full_dump] HTTP 429 при скачивании полного дампа, ждём 2 сек и повторяем...")
                    time.sleep(2)
                    continue
                r.raise_for_status()

                tmp = dest.with_suffix(dest.suffix + ".part")
                bytes_written = 0
                with tmp.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if not chunk:
                            continue
                        f.write(chunk)
                        bytes_written += len(chunk)

                os.replace(tmp, dest)

            # Быстро прочитаем last_update из головы файла (она рядом с началом)
            last_update = 0
            try:
                with dest.open("rb") as f:
                    head = f.read(2 * 1024 * 1024)
                m = head.find(b'"last_update"')
                if m != -1:
                    # грубо вытащим число после ':'
                    sub = head[m:m + 200]
                    # b'"last_update": 1721675822'
                    import re as _re
                    mm = _re.search(br'"last_update"\s*:\s*(\d+)', sub)
                    if mm:
                        last_update = int(mm.group(1))
            except Exception:
                last_update = 0

            return bytes_written, last_update
        except requests.RequestException as e:
            print(f"[full_dump] Ошибка скачивания: {e}. Повтор через 5 сек...")
            time.sleep(5)


def _seek_items_array_offset(path: Path) -> int:
    """
    Возвращает файловый offset (в байтах) сразу после символа '[' массива items.
    """
    with path.open("rb") as f:
        # items и '[' обычно в начале; читаем кусками до 64MB
        max_scan = 64 * 1024 * 1024
        buf = b""
        read = 0
        while read < max_scan:
            chunk = f.read(2 * 1024 * 1024)
            if not chunk:
                break
            buf += chunk
            read += len(chunk)

            idx = buf.find(b'"items"')
            if idx != -1:
                brack = buf.find(b"[", idx)
                if brack != -1:
                    # offset абсолютный: (прочитано - len(buf)) + brack + 1
                    # но buf содержит все прочитанные байты
                    abs_offset = brack + 1
                    return abs_offset

        raise RuntimeError("Не найден массив 'items' в полном дампе (неожиданный формат).")



def iter_dump_items(path: Path) -> Iterator[bytes]:
    """
    Потоково итерирует JSON-объекты из массива items: [{...},{...},...]
    Возвращает bytes каждого объекта {...}.
    """
    start_offset = _seek_items_array_offset(path)

    with path.open("rb") as f:
        f.seek(start_offset)

        # ASCII codes
        QUOTE = 34      # "
        BSLASH = 92     # \
        LBRACE = 123    # {
        RBRACE = 125    # }
        RBRACK = 93     # ]

        in_str = False
        esc = False
        depth = 0
        obj = bytearray()

        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break

            for b in chunk:
                if depth == 0:
                    # вне объекта, ищем начало объекта или конец массива
                    if b == LBRACE:
                        depth = 1
                        obj = bytearray()
                        obj.append(b)
                        in_str = False
                        esc = False
                    elif b == RBRACK:
                        return
                    else:
                        continue
                else:
                    obj.append(b)

                    if in_str:
                        if esc:
                            esc = False
                        elif b == BSLASH:
                            esc = True
                        elif b == QUOTE:
                            in_str = False
                        continue

                    # вне строки
                    if b == QUOTE:
                        in_str = True
                        continue
                    if b == LBRACE:
                        depth += 1
                        continue
                    if b == RBRACE:
                        depth -= 1
                        if depth == 0:
                            yield bytes(obj)
                            obj = bytearray()
                        continue


def build_sqlite_cache(dump_json_path: Path, db_path: Path) -> int:
    """
    Строит SQLite DB с таблицей lots (1 строка = 1 лот).
    Возвращает кол-во вставленных лотов.
    """
    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=OFF;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-200000;")  # ~200MB
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE lots(
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price REAL NOT NULL,
            unlock_at TEXT,
            created_at TEXT,
            item_float TEXT,
            stickers TEXT
        );
    """)
    cur.execute("CREATE INDEX idx_lots_name ON lots(name);")
    cur.execute("CREATE INDEX idx_lots_name_price ON lots(name, price);")
    cur.execute("CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT);")

    batch = []
    inserted = 0
    t0 = time.time()
    cur.execute("BEGIN;")

    for raw in iter_dump_items(dump_json_path):
        try:
            item = orjson.loads(raw)
        except Exception:
            continue

        try:
            lot_id = int(item["id"])
            name = str(item["name"])
            price = float(item["price"])
        except Exception:
            continue

        unlock_at = item.get("unlock_at")
        created_at = item.get("created_at")
        item_float = item.get("item_float")
        stickers = item.get("stickers") or []

        # stickers храним как json-строку, чтобы потом достать name/wear
        try:
            stickers_s = orjson.dumps(stickers).decode("utf-8")
        except Exception:
            stickers_s = "[]"

        batch.append((lot_id, name, price, unlock_at, created_at, item_float, stickers_s))

        if len(batch) >= 5000:
            cur.executemany(
                "INSERT OR REPLACE INTO lots(id,name,price,unlock_at,created_at,item_float,stickers) VALUES(?,?,?,?,?,?,?)",
                batch,
            )
            inserted += len(batch)
            batch.clear()

    if batch:
        cur.executemany(
            "INSERT OR REPLACE INTO lots(id,name,price,unlock_at,created_at,item_float,stickers) VALUES(?,?,?,?,?,?,?)",
            batch,
        )
        inserted += len(batch)
        batch.clear()

    conn.commit()
    dt = time.time() - t0
    print(f"[full_dump] SQLite cache построен: {inserted} лотов за {dt:.1f} сек -> {db_path.name}")
    conn.close()
    return inserted


class FullDumpUpdater:
    """
    Управляет обновлением полного дампа в фоне и предоставляет путь к активной SQLite БД.
    """

    def __init__(self, root_dir: Path, refresh_seconds: int):
        self.root_dir = root_dir
        self.refresh_seconds = refresh_seconds

        self.paths = FullDumpPaths(
            json_a=root_dir / "api_csgo_full_A.json",
            json_b=root_dir / "api_csgo_full_B.json",
            db_a=root_dir / "liss_full_A.db",
            db_b=root_dir / "liss_full_B.db",
            state=root_dir / "liss_full_state.json",
        )

        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._last_started = 0
        self._switch_event = threading.Event()

        st = _read_state(self.paths.state)
        self._active = st.get("active")  # "A"/"B"/None

    def get_active_db_path(self) -> Optional[Path]:
        with self._lock:
            active = self._active
        if active == "A":
            return self.paths.db_a if self.paths.db_a.exists() else None
        if active == "B":
            return self.paths.db_b if self.paths.db_b.exists() else None
        return None

    def ensure_initial_ready(self) -> None:
        """
        Гарантирует, что активный кэш существует. Если нет — скачивает и строит синхронно.
        """
        if self.get_active_db_path() is not None:
            return
        print("[full_dump] Активный кэш отсутствует — выполняем первичную загрузку синхронно...")
        self._update_blocking()

    def wait_update_complete(self, timeout: Optional[float] = None) -> bool:
        """
        Если в фоне идёт обновление — дождаться.
        Возвращает True, если обновление завершено.
        """
        th = None
        with self._lock:
            th = self._thread
        if th and th.is_alive():
            th.join(timeout)
            return not th.is_alive()
        return True

    def get_switch_event(self) -> threading.Event:
        """Событие, которое устанавливается после переключения активного слота."""
        return self._switch_event

    def trigger_async(self) -> None:
        """
        Запустить обновление в фоне, если:
        - нет активного потока
        - прошло >= refresh_seconds с последнего старта
        """
        with self._lock:
            now = int(time.time())
            if self._thread and self._thread.is_alive():
                return
            if self._last_started and (now - self._last_started) < self.refresh_seconds:
                return
            self._last_started = now
            self._switch_event.clear()
            self._thread = threading.Thread(target=self._update_blocking, name="FullDumpUpdater", daemon=True)
            self._thread.start()

    def _update_blocking(self) -> None:
        """
        Скачивает дамп в неактивный слот, строит DB, затем переключает active.
        """
        with self._lock:
            current = self._active
            target = "B" if current == "A" else "A"

        json_path = self.paths.json_b if target == "B" else self.paths.json_a
        db_path = self.paths.db_b if target == "B" else self.paths.db_a

        print(f"[full_dump] Обновление полного дампа -> слот {target} ({json_path.name})")
        bytes_written, last_update = _download_with_retry(FULL_DUMP_URL, json_path)
        print(f"[full_dump] Скачано {bytes_written/1024/1024:.1f} MB (last_update={last_update or 'n/a'})")

        try:
            build_sqlite_cache(json_path, db_path)
        except Exception as e:
            print(f"[full_dump] Ошибка построения SQLite cache: {e}")
            return

        now = int(time.time())
        _write_state(self.paths.state, active=target, last_success=now, last_update=last_update or None)

        with self._lock:
            self._active = target

        self._switch_event.set()

        print(f"[full_dump] Активный слот переключён на {target} (db={db_path.name})")
