"""
Поиск лотов по точному названию из локального кэша полного дампа LIS-SKINS.

ВАЖНО:
- market/search больше не используем
- данные берём из SQLite-кэша, построенного из api_csgo_full.json
- фильтрация:
  1) price <= max_price (мин-цена НЕ фильтруется)
  2) hold_days <= LISS_MAX_HOLD_DAYS
  3) если превышаем QUANTITY_MAX_ALLOWED или SUM_MAX_ALLOWED -> сортировка (price asc, hold asc)
     и берём префикс, пока условия выполняются

Промежуточные файлы пишутся в ./filtr_pars:
- 0filtr_{name}.txt: исходные лоты по имени (возможна обрезка для экономии)
- 1filtr_{name}.txt: после фильтра price+hold (возможна обрезка)
- 2filtr_{name}.txt: после ограничения qty/sum
"""

from __future__ import annotations

import json
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import orjson

import config

# чтобы не создавать гигантские debug-файлы на некоторых предметах
MAX_DEBUG_SAVE = 5000


@dataclass(frozen=True)
class LissAccount:
    name: str
    api_key: str
    partner: str
    token: str


def _project_root() -> Path:
    # предполагаем запуск из корня проекта
    return Path.cwd()


def _sanitize_filename(name: str) -> str:
    """
    Делает безопасное имя файла (особенно для Windows).
    Удаляем запрещённые символы: <>:"/\\|?* и управляющие.
    Также убираем хвостовые пробелы/точки и ограничиваем длину.
    """
    invalid = r'<>:"/\\|?*'
    cleaned = []
    for ch in str(name):
        o = ord(ch)
        if ch in invalid or o < 32:
            cleaned.append("_")
        else:
            cleaned.append(ch)

    s = "".join(cleaned).strip()
    # Windows не любит завершающие пробелы/точки
    s = s.rstrip(" .")

    if not s:
        s = "item"

    # ограничение длины имени файла (чтобы не упереться в лимиты путей)
    if len(s) > 150:
        s = s[:150].rstrip(" .")

    return s



def _ensure_filtr_dir() -> Path:
    d = _project_root() / "filtr_pars"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _save_debug(stage: int, item_name: str, payload: Any) -> None:
    d = _ensure_filtr_dir()
    fname = f"{stage}filtr_{_sanitize_filename(item_name)}.txt"
    path = d / fname
    try:
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return
    except Exception:
        try:
            path.write_text(orjson.dumps(payload).decode("utf-8"), encoding="utf-8")
            return
        except OSError:
            # На Windows некоторые имена/пути всё равно могут быть невалидны —
            # не валим основной поток логики из-за debug-файла.
            return



def _parse_unlock_at(unlock_at: Optional[str]) -> Optional[datetime]:
    if not unlock_at:
        return None
    # "2024-07-22T11:31:01.000000Z" -> "+00:00"
    s = unlock_at.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _hold_days_from_unlock(unlock_at: Optional[str], now_utc: datetime) -> int:
    """
    Считает кол-во дней холда от текущего времени UTC до unlock_at.
    Округление всегда вверх (ceil), как в ТЗ.
    """
    if not unlock_at:
        return 0
    dt = _parse_unlock_at(unlock_at)
    if dt is None:
        return 0
    if dt.tzinfo is None:
        # считаем, что это UTC
        dt = dt.replace(tzinfo=timezone.utc)
    delta = dt - now_utc
    seconds = delta.total_seconds()
    if seconds <= 0:
        return 0
    return int(math.ceil(seconds / 86400.0))


def _max_price_from_rec_price(rec_price: float) -> float:
    # LISS_MAX_PRICE = (rec_price*0.8697)/(LISS_MIN_PROFIT + 1)
    return (float(rec_price) * 0.8697) / (float(config.LISS_MIN_PROFIT) + 1.0)


def _open_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _fetch_lots_exact_name(db_path: Path, item_name: str, max_price: float) -> List[Dict[str, Any]]:
    """
    Достаём лоты по exact name и price<=max_price (чтобы не тащить слишком много).
    Дальше hold отфильтруем в питоне.
    """
    conn = _open_db(db_path)
    try:
        cur = conn.cursor()
        rows = cur.execute(
            """
            SELECT id,name,price,unlock_at,created_at,item_float,stickers
            FROM lots
            WHERE name=? AND price<=?
            ORDER BY price ASC
            """,
            (item_name, float(max_price)),
        ).fetchall()

        lots: List[Dict[str, Any]] = []
        for r in rows:
            try:
                stickers = orjson.loads((r["stickers"] or "[]").encode("utf-8"))
            except Exception:
                stickers = []
            lots.append(
                {
                    "id": int(r["id"]),
                    "name": str(r["name"]),
                    "price": float(r["price"]),
                    "unlock_at": r["unlock_at"],
                    "created_at": r["created_at"],
                    "item_float": r["item_float"],
                    "stickers": stickers,
                }
            )
        return lots
    finally:
        conn.close()


def search_lis_skins(
    *,
    item_name: str,
    rec_price: float,
    quantity_max_allowed: float,
    sum_max_allowed: float,
    db_path: Path,
) -> List[Dict[str, Any]]:
    """
    Возвращает список лотов после 2-й фильтрации (готовых к покупке).
    Каждый лот содержит: id, name, price, unlock_at, created_at, item_float, stickers, hold_days, hold.
    """
    print(f"[lis_search] Поиск лотов для точного имени: {item_name!r}")
    max_price = _max_price_from_rec_price(rec_price)
    print(f"[lis_search] max_price по формуле = {max_price:.6f} (rec_price={rec_price})")
    print(f"[lis_search] MIN price не применяем (LISS_EXTRA_MIN_PRICE={getattr(config, 'LISS_EXTRA_MIN_PRICE', None)})")

    if not db_path.exists():
        print(f"[lis_search][WARN] DB кэш не найден: {db_path}. Возвращаем 0 лотов.")
        return []

    # 0) достаём по имени + price<=max_price (уже первичный фильтр)
    lots_raw = _fetch_lots_exact_name(db_path, item_name, max_price=max_price)
    print(f"[lis_search] Из кэша получено {len(lots_raw)} лотов по price<=max_price")

    # debug 0
    if len(lots_raw) <= MAX_DEBUG_SAVE:
        _save_debug(0, item_name, {"data": lots_raw, "source": "full_dump_cache", "db": str(db_path)})
    else:
        _save_debug(0, item_name, {"data": lots_raw[:MAX_DEBUG_SAVE], "truncated": True, "total": len(lots_raw)})

    # 1) фильтр по холду (<= LISS_MAX_HOLD_DAYS)
    max_hold = int(getattr(config, "LISS_MAX_HOLD_DAYS", 0) or 0)
    now_utc = datetime.now(timezone.utc)  # один раз на весь проход (важно для скорости)
    filtered: List[Dict[str, Any]] = []
    for lot in lots_raw:
        hold_days = _hold_days_from_unlock(lot.get("unlock_at"), now_utc)
        if hold_days <= max_hold:
            lot2 = dict(lot)
            lot2["hold_days"] = hold_days
            lot2["hold"] = hold_days  # для совместимости с остальным кодом
            filtered.append(lot2)

    print(f"[lis_search] После фильтра холда (<= {max_hold}) осталось {len(filtered)} лотов")

    # debug 1 (с обрезкой)
    if len(filtered) <= MAX_DEBUG_SAVE:
        _save_debug(1, item_name, {"data": filtered, "max_hold_days": max_hold})
    else:
        _save_debug(
            1,
            item_name,
            {"data": filtered[:MAX_DEBUG_SAVE], "truncated": True, "total": len(filtered), "max_hold_days": max_hold},
        )

    # 2) проверка qty/sum
    total_qty = len(filtered)
    total_sum = sum(float(x["price"]) for x in filtered)

    print(f"[lis_search] qty={total_qty}, sum={total_sum:.6f} (лимиты qty<={quantity_max_allowed}, sum<={sum_max_allowed})")

    if total_qty <= quantity_max_allowed and total_sum <= sum_max_allowed:
        print("[lis_search] Лоты уже в пределах лимитов — сортировка/обрезка не требуется.")
        _save_debug(2, item_name, {"data": filtered, "selected": len(filtered), "sum": total_sum})
        return filtered

    # 3) сортировка (price asc, hold asc) и берём префикс под лимиты
    filtered.sort(key=lambda x: (float(x["price"]), int(x.get("hold_days", 0))))
    selected: List[Dict[str, Any]] = []
    running_sum = 0.0
    for lot in filtered:
        if len(selected) + 1 > quantity_max_allowed:
            break
        next_sum = running_sum + float(lot["price"])
        if next_sum > sum_max_allowed:
            break
        selected.append(lot)
        running_sum = next_sum

    print(f"[lis_search] После обрезки под лимиты выбрано {len(selected)} лотов, сумма={running_sum:.6f}")
    _save_debug(2, item_name, {"data": selected, "selected": len(selected), "sum": running_sum})
    return selected
