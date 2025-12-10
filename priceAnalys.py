# priceAnalys.py - core logic: downloading Steam pages, parsing sales, analysing price

import os
import re
import json
import time
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import unquote

import requests

import config


# Состояние прямого IP для логики отдыха (как для прокси)
direct_ip_state = {
    "rest_until_ts": 0.0,
    "fail_stage": 0,
}


@dataclass
class Sale:
    dt: datetime
    price: float
    amount: int


# ---------- Filesystem helpers ----------

def ensure_directories() -> None:
    for path in [
        config.HTML_APPROVE_DIR,
        config.HTML_BLACKLIST_DIR,
        config.HTML_FAILED_DIR,
        config.SELL_PARSING_DIR,
        config.HTML_TEMP_DIR,
    ]:
        os.makedirs(path, exist_ok=True)


def safe_filename(name: str) -> str:
    # Заменяем всё, кроме букв/цифр/._- на "_"
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name)


# ---------- DB helpers ----------

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(config.DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_proxy_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(config.PROXY_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_blacklist_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(config.BLACKLIST_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_rec_price_blacklist_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(config.REC_PRICE_BLACKLIST_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    ensure_directories()
    conn = get_conn()
    cur = conn.cursor()

    # Таблица с результатами по предметам
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS steam_items (
            item_name      TEXT PRIMARY KEY,
            game_code      INTEGER,
            rec_price      REAL,
            avg_sales      REAL,
            tier           INTEGER,
            graph_type     TEXT,
            updated_at     TEXT,
            sales_points   INTEGER,
            purchased_lots REAL DEFAULT 0,
            purchased_sum  REAL DEFAULT 0
        )
        """
    )

    # Добавляем новые поля, если таблица уже существовала
    cur.execute("PRAGMA table_info(steam_items)")
    existing_columns = {row[1] for row in cur.fetchall()}
    if "purchased_lots" not in existing_columns:
        cur.execute(
            "ALTER TABLE steam_items ADD COLUMN purchased_lots REAL DEFAULT 0"
        )
    if "purchased_sum" not in existing_columns:
        cur.execute(
            "ALTER TABLE steam_items ADD COLUMN purchased_sum REAL DEFAULT 0"
        )

    conn.commit()
    conn.close()

    # Таблица прокси в отдельной базе
    proxy_conn = get_proxy_conn()
    proxy_cur = proxy_conn.cursor()
    proxy_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS proxies (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            address             TEXT UNIQUE,
            last_used_ts        REAL,
            rest_until_ts       REAL,
            fail_stage          INTEGER DEFAULT 0,
            fallback_fail_count INTEGER DEFAULT 0,
            disabled            INTEGER DEFAULT 0
        )
        """
    )
    proxy_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS proxy_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_position INTEGER DEFAULT -1
        )
        """
    )
    proxy_cur.execute(
        "INSERT OR IGNORE INTO proxy_state(id, last_position) VALUES(1, -1)"
    )
    proxy_conn.commit()
    proxy_conn.close()

    # Таблица блэклиста в отдельной базе
    bl_conn = get_blacklist_conn()
    bl_cur = bl_conn.cursor()
    bl_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS blacklist (
            item_name  TEXT PRIMARY KEY,
            added_at   TEXT,
            expires_at TEXT,
            reason     TEXT
        )
        """
    )
    bl_conn.commit()
    bl_conn.close()

    # Таблица блэклиста по рек. цене в отдельной базе
    rec_bl_conn = get_rec_price_blacklist_conn()
    rec_bl_cur = rec_bl_conn.cursor()
    rec_bl_cur.execute(
        """
        CREATE TABLE IF NOT EXISTS blacklist (
            item_name  TEXT PRIMARY KEY,
            added_at   TEXT,
            expires_at TEXT,
            reason     TEXT
        )
        """
    )
    rec_bl_conn.commit()
    rec_bl_conn.close()


def upsert_proxy(address: str) -> None:
    conn = get_proxy_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO proxies(address, last_used_ts, rest_until_ts, fail_stage,
                                      fallback_fail_count, disabled)
        VALUES(?, 0, 0, 0, 0, 0)
        """,
        (address,),
    )
    conn.commit()
    conn.close()


def load_proxies_from_file() -> None:
    """
    Загружает список прокси из proxies.txt в таблицу proxies.
    Формат строки: ip:port@password:user
    """
    if config.PROXY_SELECT == 0:
        return

    if not os.path.exists(config.PROXIES_FILE):
        print(f"[PROXY] Файл {config.PROXIES_FILE} не найден, прокси не загружены.")
        return

    with open(config.PROXIES_FILE, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]

    for line in lines:
        upsert_proxy(line)

    print(f"[PROXY] Загружено {len(lines)} прокси из {config.PROXIES_FILE}.")


def get_cached_item(item_name: str) -> Optional[sqlite3.Row]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM steam_items WHERE item_name = ?",
        (item_name,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def save_item_result(
    item_name: str,
    game_code: int,
    rec_price: float,
    avg_sales: float,
    tier: int,
    graph_type: str,
    sales_points: int,
) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO steam_items(item_name, game_code, rec_price, avg_sales,
                                tier, graph_type, updated_at, sales_points)
        VALUES(?,?,?,?,?,?,?,?)
        ON CONFLICT(item_name) DO UPDATE SET
            rec_price    = excluded.rec_price,
            avg_sales    = excluded.avg_sales,
            tier         = excluded.tier,
            graph_type   = excluded.graph_type,
            updated_at   = excluded.updated_at,
            sales_points = excluded.sales_points
        """,
        (
            item_name,
            game_code,
            rec_price,
            avg_sales,
            tier,
            graph_type,
            datetime.utcnow().isoformat(timespec="seconds"),
            sales_points,
        ),
    )
    conn.commit()
    conn.close()


def _select_blacklist_conn(rec_price: bool) -> sqlite3.Connection:
    return get_rec_price_blacklist_conn() if rec_price else get_blacklist_conn()


def add_to_blacklist(
    item_name: str, reason: str, days: Optional[float] = None, *, rec_price: bool = False
) -> None:
    now = datetime.utcnow()
    duration_days = config.BLACKLIST_DAYS if days is None else days
    expires = now + timedelta(days=duration_days)
    conn = _select_blacklist_conn(rec_price)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO blacklist(item_name, added_at, expires_at, reason)
        VALUES(?,?,?,?)
        ON CONFLICT(item_name) DO UPDATE SET
            added_at   = excluded.added_at,
            expires_at = excluded.expires_at,
            reason     = excluded.reason
        """,
        (item_name, now.isoformat(timespec="seconds"), expires.isoformat(timespec="seconds"), reason),
    )
    conn.commit()
    conn.close()


def _base_price_blacklist_redirect(
    reason: str, base_price: Optional[float], days: Optional[float]
) -> Tuple[bool, Optional[float], str]:
    """Проверяет пороги по base_price и возвращает параметры для блэклиста по рек. цене."""

    if base_price is None:
        return False, days, reason

    if base_price < config.MIN_REC_PRICE_USD1:
        new_reason = (
            "base_price_below_minimum "
            f"({base_price:.4f} < {config.MIN_REC_PRICE_USD1:.4f})"
        )
        return (
            True,
            config.MIN_REC_PRICE_BLACKLIST_DAYS1,
            f"{new_reason}; original_reason={reason}",
        )

    if base_price < config.MIN_REC_PRICE_USD2:
        new_reason = (
            "base_price_below_secondary_minimum "
            f"({base_price:.4f} < {config.MIN_REC_PRICE_USD2:.4f})"
        )
        return (
            True,
            config.MIN_REC_PRICE_BLACKLIST_DAYS2,
            f"{new_reason}; original_reason={reason}",
        )

    return False, days, reason


def blacklist_with_html(
    item_name: str,
    reason: str,
    html: str,
    days: Optional[float] = None,
    *,
    rec_price_blacklist: bool = False,
    base_price: Optional[float] = None,
) -> Dict[str, str]:
    """Добавляет предмет в блэклист и сохраняет его HTML в соответствующую папку."""

    if not rec_price_blacklist:
        redirect, days, reason = _base_price_blacklist_redirect(reason, base_price, days)
        rec_price_blacklist = redirect or rec_price_blacklist

    print(f"[ANALYSIS] {item_name}: {reason}")
    add_to_blacklist(item_name, reason, days=days, rec_price=rec_price_blacklist)
    html_path = os.path.join(
        config.HTML_BLACKLIST_DIR,
        f"{safe_filename(item_name)}.html",
    )
    try:
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html)
    except Exception:
        pass

    return {
        "status": "blacklist",
        "item_name": item_name,
        "reason": reason,
    }


def get_blacklist_entry(item_name: str, *, rec_price: bool = False) -> Optional[sqlite3.Row]:
    conn = _select_blacklist_conn(rec_price)
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM blacklist WHERE item_name = ?",
        (item_name,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def remove_from_blacklist(item_name: str, *, rec_price: bool = False) -> None:
    conn = _select_blacklist_conn(rec_price)
    cur = conn.cursor()
    cur.execute("DELETE FROM blacklist WHERE item_name = ?", (item_name,))
    conn.commit()
    conn.close()


def get_active_blacklist_entry(item_name: str) -> Optional[Dict[str, object]]:
    sources = (
        (False, "general_blacklist"),
        (True, "rec_price_blacklist"),
    )
    now = datetime.utcnow()
    for rec_price_flag, source_name in sources:
        entry = get_blacklist_entry(item_name, rec_price=rec_price_flag)
        if entry is None:
            continue

        try:
            expires_at_str = entry["expires_at"]
        except Exception:
            expires_at_str = None
        try:
            expires_at = datetime.fromisoformat(expires_at_str) if expires_at_str else None
        except Exception:
            expires_at = None

        if expires_at is not None and expires_at > now:
            return {"entry": entry, "source": source_name, "rec_price": rec_price_flag}

        remove_from_blacklist(item_name, rec_price=rec_price_flag)

    return None


# ---------- Weighted statistics ----------

def weighted_mean(values: List[float], weights: List[float]) -> float:
    total_w = sum(weights)
    if total_w <= 0:
        return 0.0
    return sum(v * w for v, w in zip(values, weights)) / total_w


def weighted_std(values: List[float], weights: List[float]) -> float:
    total_w = sum(weights)
    if total_w <= 0:
        return 0.0
    m = weighted_mean(values, weights)
    var = sum(w * (v - m) ** 2 for v, w in zip(values, weights)) / total_w
    return math.sqrt(var)


def weighted_quantile(values: List[float], weights: List[float], q: float) -> float:
    """
    Взвешенный перцентиль:
    q в [0,1] – доля ОБЪЁМА, которая должна быть НИЖЕ результата.
    """
    assert 0.0 <= q <= 1.0
    if not values:
        return 0.0

    data = sorted(zip(values, weights), key=lambda x: x[0])
    total_w = sum(w for _, w in data)
    if total_w <= 0:
        mid = len(data) // 2
        return data[mid][0]

    target = total_w * q
    acc = 0.0
    for v, w in data:
        acc += w
        if acc >= target:
            return v
    return data[-1][0]


def compute_basic_metrics(sales: List[Sale]) -> Dict[str, float]:
    """
    Возвращает:
      - base_price: взвешенная медиана
      - mean_price: взвешенное среднее
      - std_price, cv_price
      - p10, p20, p40, p60, p80 (взвешенные)
      - trend_rel_30: относительный тренд за 30 дней (по точкам)
      - trend_rel_30_unweighted: относительный тренд за 30 дней (синоним trend_rel_30)
      - trend_rel_30_volume: относительный тренд за 30 дней (взвешенный по объёму)
      - trend_rel_recent: относительный тренд за последние FORECAST_TREND_WINDOW_DAYS (по точкам)
      - trend_rel_30_down_forecast: выбранный нисходящий тренд (осторожный минимум по модулю между 30д и свежим окном)
      - slope_per_day: наклон линейной регрессии (в валюте/день, по точкам)
      - slope_per_day_unweighted: наклон линейной регрессии (синоним slope_per_day)
      - slope_per_day_volume: наклон линейной регрессии (в валюте/день, взвешенный)
      - slope_per_day_recent: наклон для свежего окна (по точкам)
      - slope_per_day_down_forecast: наклон выбранного нисходящего тренда
    """

    def _linear_regression_params(
        t_values: List[float], prices_values: List[float], weights_values: List[float]
    ) -> Tuple[float, float]:
        sum_w = sum(weights_values)
        if sum_w <= 0:
            return 0.0, 0.0

        sum_wt = sum(w * t for t, w in zip(t_values, weights_values))
        sum_wp = sum(w * p for p, w in zip(prices_values, weights_values))
        sum_wt2 = sum(w * (t ** 2) for t, w in zip(t_values, weights_values))
        sum_wtp = sum(w * t * p for t, p, w in zip(t_values, prices_values, weights_values))

        denom = (sum_w * sum_wt2 - sum_wt ** 2)
        if abs(denom) < 1e-9:
            return 0.0, 0.0

        slope = (sum_w * sum_wtp - sum_wt * sum_wp) / denom
        intercept = (sum_wp - slope * sum_wt) / sum_w
        return slope, intercept

    if not sales:
        return {
            "base_price": 0.0,
            "mean_price": 0.0,
            "std_price": 0.0,
            "cv_price": 0.0,
            "p10": 0.0,
            "p20": 0.0,
            "p40": 0.0,
            "p60": 0.0,
            "p80": 0.0,
            "p20_resid": 0.0,
            "p80_resid": 0.0,
            "wave_amp_raw": 0.0,
            "wave_amp_detrended": 0.0,
            "trend_rel_30": 0.0,
            "trend_rel_30_unweighted": 0.0,
            "trend_rel_30_volume": 0.0,
            "trend_rel_recent": 0.0,
            "trend_rel_30_down_forecast": 0.0,
            "slope_per_day": 0.0,
            "slope_per_day_unweighted": 0.0,
            "slope_per_day_volume": 0.0,
            "slope_per_day_recent": 0.0,
            "slope_per_day_down_forecast": 0.0,
            "intercept_price": 0.0,
            "intercept_price_volume": 0.0,
        }

    sales_sorted = sorted(sales, key=lambda s: s.dt)
    prices_raw = [s.price for s in sales_sorted]
    weights = [s.amount for s in sales_sorted]

    def _price_corridor(values: List[float], weights_values: List[float]) -> Tuple[float, float]:
        method = str(getattr(config, "PRICE_OUTLIER_METHOD", "quantile")).lower()
        if method == "iqr":
            q1 = weighted_quantile(values, weights_values, 0.25)
            q3 = weighted_quantile(values, weights_values, 0.75)
            iqr = q3 - q1
            mult = float(getattr(config, "PRICE_OUTLIER_IQR_MULT", 1.5))
            low = q1 - mult * iqr
            high = q3 + mult * iqr
        else:
            low_q, high_q = getattr(config, "PRICE_OUTLIER_QUANTILES", (0.05, 0.95))
            low_q = max(0.0, min(1.0, float(low_q)))
            high_q = max(0.0, min(1.0, float(high_q)))
            if low_q > high_q:
                low_q, high_q = high_q, low_q
            low = weighted_quantile(values, weights_values, low_q)
            high = weighted_quantile(values, weights_values, high_q)

        if low > high:
            low, high = high, low
        return low, high

    low_corridor, high_corridor = _price_corridor(prices_raw, weights)
    prices = [
        max(low_corridor, min(price, high_corridor))
        for price in prices_raw
    ]

    sales_clean = [
        Sale(dt=s.dt, price=p, amount=s.amount) for s, p in zip(sales_sorted, prices)
    ]

    base_price = weighted_quantile(prices, weights, 0.5)
    mean_price = weighted_mean(prices, weights)
    std_price = weighted_std(prices, weights)
    cv_price = std_price / mean_price if mean_price > 0 else 0.0

    p10 = weighted_quantile(prices, weights, 0.10)
    p20 = weighted_quantile(prices, weights, 0.20)
    p40 = weighted_quantile(prices, weights, 0.40)
    p60 = weighted_quantile(prices, weights, 0.60)
    p80 = weighted_quantile(prices, weights, 0.80)

    first_dt = sales_sorted[0].dt
    t_vals = [(s.dt - first_dt).total_seconds() / 86400.0 for s in sales_clean]

    slope_volume, intercept_volume = _linear_regression_params(
        t_vals, prices, weights
    )
    slope_points, intercept_points = _linear_regression_params(
        t_vals, prices, [1.0 for _ in prices]
    )

    if base_price > 0:
        trend_rel_30_points = (slope_points * 30.0) / base_price
        trend_rel_30_volume = (slope_volume * 30.0) / base_price
    else:
        trend_rel_30_points = 0.0
        trend_rel_30_volume = 0.0

    def _recent_trend(window_days: float) -> Tuple[float, float]:
        if window_days <= 0:
            return 0.0, 0.0

        last_dt = sales_clean[-1].dt
        cutoff = last_dt - timedelta(days=window_days)
        window_sales = [s for s in sales_clean if s.dt >= cutoff]
        if len(window_sales) < 2:
            window_sales = sales_clean

        first_dt_local = window_sales[0].dt
        t_vals_local = [
            (s.dt - first_dt_local).total_seconds() / 86400.0
            for s in window_sales
        ]
        prices_local = [s.price for s in window_sales]
        slope_local, _ = _linear_regression_params(
            t_vals_local, prices_local, [1.0 for _ in prices_local]
        )

        if base_price > 0:
            trend_rel_local = (slope_local * 30.0) / base_price
        else:
            trend_rel_local = 0.0

        return trend_rel_local, slope_local

    trend_rel_30_recent, slope_points_recent = _recent_trend(
        config.FORECAST_TREND_WINDOW_DAYS
    )

    # Для прогноза в нисходящем тренде выбираем наибольший (с учётом знака)
    # тренд между основным 30-дневным и свежим (последние X дней).
    if trend_rel_30_points >= trend_rel_30_recent:
        trend_rel_forecast = trend_rel_30_points
        slope_forecast = slope_points
    else:
        trend_rel_forecast = trend_rel_30_recent
        slope_forecast = slope_points_recent

    # Прогноз применяем только при нисходящем тренде: для нейтральных/положительных
    # трендов не хотим повышать цену.
    if trend_rel_forecast >= 0:
        trend_rel_forecast = 0.0
        slope_forecast = 0.0

    residuals = [
        price - (slope_points * t + intercept_points)
        for price, t in zip(prices, t_vals)
    ]
    p20_resid = weighted_quantile(residuals, weights, 0.20)
    p80_resid = weighted_quantile(residuals, weights, 0.80)

    wave_amp_raw = (p80 - p20) / base_price if base_price > 0 else 0.0
    wave_amp_detrended = (p80_resid - p20_resid) / base_price if base_price > 0 else 0.0

    return {
        "base_price": base_price,
        "mean_price": mean_price,
        "std_price": std_price,
        "cv_price": cv_price,
        "p10": p10,
        "p20": p20,
        "p40": p40,
        "p60": p60,
        "p80": p80,
        "p20_resid": p20_resid,
        "p80_resid": p80_resid,
        "wave_amp_raw": wave_amp_raw,
        "wave_amp_detrended": wave_amp_detrended,
        "trend_rel_30": trend_rel_30_points,
        "trend_rel_30_unweighted": trend_rel_30_points,
        "trend_rel_30_volume": trend_rel_30_volume,
        "trend_rel_recent": trend_rel_30_recent,
        "trend_rel_30_down_forecast": trend_rel_forecast,
        "slope_per_day": slope_points,
        "slope_per_day_unweighted": slope_points,
        "slope_per_day_volume": slope_volume,
        "slope_per_day_recent": slope_points_recent,
        "slope_per_day_down_forecast": slope_forecast,
        "intercept_price": intercept_points,
        "intercept_price_volume": intercept_volume,
    }


def gap_stats_between_points(
    sales: List[Sale], window_days: float, min_gap_hours: float
) -> Tuple[float, int]:
    """
    Возвращает:
      - максимальный промежуток (в часах) между соседними точками продаж
        за последние window_days дней;
      - количество гэпов длительностью >= min_gap_hours в этом окне.
    """
    if len(sales) < 2:
        return 0.0, 0

    sales_sorted = sorted(sales, key=lambda s: s.dt)
    cutoff_dt = sales_sorted[-1].dt - timedelta(days=window_days)
    filtered_sales = [s for s in sales_sorted if s.dt >= cutoff_dt]

    if len(filtered_sales) < 2:
        return 0.0, 0

    max_gap = 0.0
    long_gap_count = 0
    prev_dt = filtered_sales[0].dt
    for s in filtered_sales[1:]:
        gap_hours = (s.dt - prev_dt).total_seconds() / 3600.0
        if gap_hours > max_gap:
            max_gap = gap_hours
        if gap_hours >= min_gap_hours:
            long_gap_count += 1
        prev_dt = s.dt

    return max_gap, long_gap_count


# ---------- Просадки (dips) ----------

def compute_price_dips(sales: List[Sale], metrics: Dict[str, float]) -> Dict[str, float]:
    """
    Считает суммарную длительность валидных провалов в старой и свежей частях графика.
    Валидный провал:
      - подряд не меньше DIP_MIN_POINTS точек
      - цена каждой точки ниже base_price * (1 - LOW_CORRIDOR_REL_...)
      - суммарный объём провала >= (medium_volume_per_point * N_points * DIP_VOLUME_FACTOR)
    Возвращает:
      - old_dips_days
      - recent_dips_days
    """
    if not sales:
        return {"old_dips_days": 0.0, "recent_dips_days": 0.0}

    base_price = metrics["base_price"]
    trend_rel_30 = metrics.get("trend_rel_30", 0.0)
    slope_per_day = metrics.get("slope_per_day", 0.0)
    if base_price <= 0:
        return {"old_dips_days": 0.0, "recent_dips_days": 0.0}

    sales_sorted = sorted(sales, key=lambda s: s.dt)
    first_dt = sales_sorted[0].dt
    last_dt = sales_sorted[-1].dt
    last_t = (last_dt - first_dt).total_seconds() / 86400.0

    recent_median_price = base_price
    cutoff_recent_median = last_dt - timedelta(days=config.RECENT_DIP_MEDIAN_DAYS)
    recent_prices = [s.price for s in sales_sorted if s.dt >= cutoff_recent_median]
    recent_weights = [s.amount for s in sales_sorted if s.dt >= cutoff_recent_median]
    if recent_prices and recent_weights:
        recent_median_price = weighted_quantile(recent_prices, recent_weights, 0.5)

    volumes = [s.amount for s in sales_sorted]
    medium_volume = sum(volumes) / len(volumes) if volumes else 0.0

    old_dips_days = 0.0
    recent_dips_days = 0.0

    is_downtrend = trend_rel_30 < -config.TREND_REL_FLAT_MAX

    def _should_apply_trend_adj(bucket: str, age_days_val: float) -> bool:
        if bucket != "old":
            return True
        if not is_downtrend:
            return True
        # При нисходящем тренде учитываем поправку для старых точек только
        # в первые ~15 дней от последней продажи
        return age_days_val <= 15.0

    n = len(sales_sorted)
    i = 0
    while i < n:
        s = sales_sorted[i]
        age_days = (last_dt - s.dt).total_seconds() / 86400.0

        if age_days <= config.DIP_RECENT_WINDOW_DAYS:
            low_corridor_rel = config.LOW_CORRIDOR_REL_RECENT
            target_bucket = "recent"
            bucket_base_price = recent_median_price if recent_median_price > 0 else base_price

            # Учитываем ожидаемое снижение цены из-за нисходящего тренда,
            # чтобы стабильные downtrend-графики не попадали в recent_deep_dips.
            trend_factor = 1.0 + min(trend_rel_30, 0.0) * (
                config.DIP_RECENT_WINDOW_DAYS / 30.0
            )
            trend_factor = max(trend_factor, 0.0)
        else:
            low_corridor_rel = config.LOW_CORRIDOR_REL_OLD
            target_bucket = "old"
            trend_factor = 1.0
            bucket_base_price = base_price

        price_threshold_with_trend = (
            bucket_base_price * trend_factor * (1.0 - low_corridor_rel)
        )
        price_threshold_no_trend = bucket_base_price * (1.0 - low_corridor_rel)

        t_value = (s.dt - first_dt).total_seconds() / 86400.0
        trend_adjustment = (
            slope_per_day * (t_value - last_t)
            if _should_apply_trend_adj(target_bucket, age_days)
            else 0.0
        )
        price_with_trend = s.price - trend_adjustment
        price_no_trend = s.price

        if not (
            price_with_trend < price_threshold_with_trend
            and price_no_trend < price_threshold_no_trend
        ):
            i += 1
            continue

        dip_start_idx = i
        dip_end_idx = i
        dip_volume = s.amount

        j = i + 1
        while j < n:
            s_j = sales_sorted[j]
            age_days_j = (last_dt - s_j.dt).total_seconds() / 86400.0
            if (age_days_j <= config.DIP_RECENT_WINDOW_DAYS and target_bucket == "recent") or \
               (age_days_j > config.DIP_RECENT_WINDOW_DAYS and target_bucket == "old"):
                t_value_j = (s_j.dt - first_dt).total_seconds() / 86400.0
                trend_adjustment_j = (
                    slope_per_day * (t_value_j - last_t)
                    if _should_apply_trend_adj(target_bucket, age_days_j)
                    else 0.0
                )
                price_with_trend_j = s_j.price - trend_adjustment_j
                price_no_trend_j = s_j.price

                if (
                    price_with_trend_j < price_threshold_with_trend
                    and price_no_trend_j < price_threshold_no_trend
                ):
                    dip_end_idx = j
                    dip_volume += s_j.amount
                    j += 1
                    continue
            break

        num_points = dip_end_idx - dip_start_idx + 1

        if num_points >= config.DIP_MIN_POINTS and medium_volume > 0:
            required_volume = medium_volume * num_points * config.DIP_VOLUME_FACTOR
            if dip_volume >= required_volume:
                dt_start = sales_sorted[dip_start_idx].dt
                dt_end = sales_sorted[dip_end_idx].dt
                dip_days = (dt_end - dt_start).total_seconds() / 86400.0
                if target_bucket == "recent":
                    recent_dips_days += max(dip_days, 0.0)
                else:
                    old_dips_days += max(dip_days, 0.0)

        i = dip_end_idx + 1

    return {"old_dips_days": old_dips_days, "recent_dips_days": recent_dips_days}


def compute_old_dip_histogram(
    sales: List[Sale], metrics: Dict[str, float], *, bins_count: int = 10, bin_size_days: float = 3.0
) -> List[float]:
    """
    Строит гистограмму длительностей old_dip-провалов по диапазонам времени от последней продажи.

    - bins_count: количество диапазонов (по умолчанию 10)
    - bin_size_days: ширина диапазона в днях (по умолчанию 3 дня → покрываем ~30 дней)
    """

    if not sales or bins_count <= 0 or bin_size_days <= 0:
        return [0.0 for _ in range(max(bins_count, 0))]

    base_price = metrics["base_price"]
    trend_rel_30 = metrics.get("trend_rel_30", 0.0)
    slope_per_day = metrics.get("slope_per_day", 0.0)
    if base_price <= 0:
        return [0.0 for _ in range(bins_count)]

    sales_sorted = sorted(sales, key=lambda s: s.dt)
    first_dt = sales_sorted[0].dt
    last_dt = sales_sorted[-1].dt
    last_t = (last_dt - first_dt).total_seconds() / 86400.0

    recent_median_price = base_price
    cutoff_recent_median = last_dt - timedelta(days=config.RECENT_DIP_MEDIAN_DAYS)
    recent_prices = [s.price for s in sales_sorted if s.dt >= cutoff_recent_median]
    recent_weights = [s.amount for s in sales_sorted if s.dt >= cutoff_recent_median]
    if recent_prices and recent_weights:
        recent_median_price = weighted_quantile(recent_prices, recent_weights, 0.5)

    volumes = [s.amount for s in sales_sorted]
    medium_volume = sum(volumes) / len(volumes) if volumes else 0.0

    histogram = [0.0 for _ in range(bins_count)]
    max_age = bins_count * bin_size_days

    is_downtrend = trend_rel_30 < -config.TREND_REL_FLAT_MAX

    def _should_apply_trend_adj(bucket: str, age_days_val: float) -> bool:
        if bucket != "old":
            return True
        if not is_downtrend:
            return True
        return age_days_val <= 15.0

    n = len(sales_sorted)
    i = 0
    while i < n:
        s = sales_sorted[i]
        age_days = (last_dt - s.dt).total_seconds() / 86400.0

        if age_days <= config.DIP_RECENT_WINDOW_DAYS:
            low_corridor_rel = config.LOW_CORRIDOR_REL_RECENT
            target_bucket = "recent"
            bucket_base_price = (
                recent_median_price if recent_median_price > 0 else base_price
            )
            trend_factor = 1.0 + min(trend_rel_30, 0.0) * (
                config.DIP_RECENT_WINDOW_DAYS / 30.0
            )
            trend_factor = max(trend_factor, 0.0)
        else:
            low_corridor_rel = config.LOW_CORRIDOR_REL_OLD
            target_bucket = "old"
            trend_factor = 1.0
            bucket_base_price = base_price

        price_threshold_with_trend = (
            bucket_base_price * trend_factor * (1.0 - low_corridor_rel)
        )
        price_threshold_no_trend = bucket_base_price * (1.0 - low_corridor_rel)

        t_value = (s.dt - first_dt).total_seconds() / 86400.0
        trend_adjustment = (
            slope_per_day * (t_value - last_t)
            if _should_apply_trend_adj(target_bucket, age_days)
            else 0.0
        )
        price_with_trend = s.price - trend_adjustment
        price_no_trend = s.price

        if not (
            price_with_trend < price_threshold_with_trend
            and price_no_trend < price_threshold_no_trend
        ):
            i += 1
            continue

        dip_start_idx = i
        dip_end_idx = i
        dip_volume = s.amount

        j = i + 1
        while j < n:
            s_j = sales_sorted[j]
            age_days_j = (last_dt - s_j.dt).total_seconds() / 86400.0
            if (age_days_j <= config.DIP_RECENT_WINDOW_DAYS and target_bucket == "recent") or (
                age_days_j > config.DIP_RECENT_WINDOW_DAYS and target_bucket == "old"
            ):
                t_value_j = (s_j.dt - first_dt).total_seconds() / 86400.0
                trend_adjustment_j = (
                    slope_per_day * (t_value_j - last_t)
                    if _should_apply_trend_adj(target_bucket, age_days_j)
                    else 0.0
                )
                price_with_trend_j = s_j.price - trend_adjustment_j
                price_no_trend_j = s_j.price

                if (
                    price_with_trend_j < price_threshold_with_trend
                    and price_no_trend_j < price_threshold_no_trend
                ):
                    dip_end_idx = j
                    dip_volume += s_j.amount
                    j += 1
                    continue
            break

        num_points = dip_end_idx - dip_start_idx + 1

        if num_points >= config.DIP_MIN_POINTS and medium_volume > 0:
            required_volume = medium_volume * num_points * config.DIP_VOLUME_FACTOR
            if dip_volume >= required_volume:
                dt_start = sales_sorted[dip_start_idx].dt
                dt_end = sales_sorted[dip_end_idx].dt
                if target_bucket == "old":
                    age_start = (last_dt - dt_end).total_seconds() / 86400.0
                    age_end = (last_dt - dt_start).total_seconds() / 86400.0
                    if age_start < max_age and age_end > 0:
                        for bin_idx in range(bins_count):
                            bin_left = bin_idx * bin_size_days
                            bin_right = bin_left + bin_size_days
                            overlap = max(0.0, min(age_end, bin_right) - max(age_start, bin_left))
                            if overlap > 0:
                                histogram[bin_idx] += overlap

        i = dip_end_idx + 1

    return histogram


def find_valid_dip_segments(sales: List[Sale], metrics: Dict[str, float]) -> List[List[Sale]]:
    """
    Находит валидные сегменты провалов (список списков Sale),
    по тем же правилам, что и compute_price_dips.
    Используется для расчёта rec_price для wave-графиков.
    """
    segments: List[List[Sale]] = []
    if not sales:
        return segments

    base_price = metrics["base_price"]
    trend_rel_30 = metrics.get("trend_rel_30", 0.0)
    slope_per_day = metrics.get("slope_per_day", 0.0)
    if base_price <= 0:
        return segments

    sales_sorted = sorted(sales, key=lambda s: s.dt)
    first_dt = sales_sorted[0].dt
    last_dt = sales_sorted[-1].dt
    last_t = (last_dt - first_dt).total_seconds() / 86400.0

    volumes = [s.amount for s in sales_sorted]
    medium_volume = sum(volumes) / len(volumes) if volumes else 0.0

    is_downtrend = trend_rel_30 < -config.TREND_REL_FLAT_MAX

    def _should_apply_trend_adj(bucket: str, age_days_val: float) -> bool:
        if bucket != "old":
            return True
        if not is_downtrend:
            return True
        return age_days_val <= 15.0

    n = len(sales_sorted)
    i = 0
    while i < n:
        s = sales_sorted[i]
        age_days = (last_dt - s.dt).total_seconds() / 86400.0

        if age_days <= config.DIP_RECENT_WINDOW_DAYS:
            low_corridor_rel = config.LOW_CORRIDOR_REL_RECENT
            target_bucket = "recent"

            trend_factor = 1.0 + min(trend_rel_30, 0.0) * (
                config.DIP_RECENT_WINDOW_DAYS / 30.0
            )
            trend_factor = max(trend_factor, 0.0)
        else:
            low_corridor_rel = config.LOW_CORRIDOR_REL_OLD
            target_bucket = "old"
            trend_factor = 1.0

        price_threshold_with_trend = (
            base_price * trend_factor * (1.0 - low_corridor_rel)
        )
        price_threshold_no_trend = base_price * (1.0 - low_corridor_rel)

        t_value = (s.dt - first_dt).total_seconds() / 86400.0
        trend_adjustment = (
            slope_per_day * (t_value - last_t)
            if _should_apply_trend_adj(target_bucket, age_days)
            else 0.0
        )
        price_with_trend = s.price - trend_adjustment
        price_no_trend = s.price

        if not (
            price_with_trend < price_threshold_with_trend
            and price_no_trend < price_threshold_no_trend
        ):
            i += 1
            continue

        dip_start_idx = i
        dip_end_idx = i
        dip_volume = s.amount

        j = i + 1
        while j < n:
            s_j = sales_sorted[j]
            age_days_j = (last_dt - s_j.dt).total_seconds() / 86400.0
            if (age_days_j <= config.DIP_RECENT_WINDOW_DAYS and target_bucket == "recent") or \
               (age_days_j > config.DIP_RECENT_WINDOW_DAYS and target_bucket == "old"):
                t_value_j = (s_j.dt - first_dt).total_seconds() / 86400.0
                trend_adjustment_j = (
                    slope_per_day * (t_value_j - last_t)
                    if _should_apply_trend_adj(target_bucket, age_days_j)
                    else 0.0
                )
                price_with_trend_j = s_j.price - trend_adjustment_j
                price_no_trend_j = s_j.price

                if (
                    price_with_trend_j < price_threshold_with_trend
                    and price_no_trend_j < price_threshold_no_trend
                ):
                    dip_end_idx = j
                    dip_volume += s_j.amount
                    j += 1
                    continue
            break

        num_points = dip_end_idx - dip_start_idx + 1
        if num_points >= config.DIP_MIN_POINTS and medium_volume > 0:
            required_volume = medium_volume * num_points * config.DIP_VOLUME_FACTOR
            if dip_volume >= required_volume:
                seg = sales_sorted[dip_start_idx: dip_end_idx + 1]
                segments.append(seg)

        i = dip_end_idx + 1

    return segments


# ---------- Рекомендованная цена ----------

def compute_rec_price_default(
    sales: List[Sale], q: float, recent_days: Optional[float] = None
) -> Tuple[float, List[Sale]]:
    """
    Базовая рек. цена:
      - последние recent_days (или RECENT_DAYS_FOR_REC_PRICE по умолчанию) дней,
      - взвешенный квантиль q (зависит от стабильности предмета).
    """
    if not sales:
        return 0.0, []

    sales_sorted = sorted(sales, key=lambda s: s.dt)
    last_dt = sales_sorted[-1].dt
    days = recent_days if recent_days is not None else config.RECENT_DAYS_FOR_REC_PRICE
    cutoff = last_dt - timedelta(days=days)

    recent = [s for s in sales_sorted if s.dt >= cutoff]
    if len(recent) < 10:
        recent = sales_sorted

    prices = [s.price for s in recent]
    weights = [s.amount for s in recent]

    return weighted_quantile(prices, weights, q), recent


def compute_stable_like_rec_price(
    sales: List[Sale], metrics: Dict[str, float]
) -> Tuple[float, List[Sale], float]:
    """
    Рассчитывает rec_price по стандартным правилам для стабильных графиков.
    Возвращает базовую цену (до прогнозного фактора), список продаж и фактор
    тренда (может быть <1 при нисходящем тренде).
    """

    trend = metrics.get("trend_rel_30", 0.0)
    recent_days = (
        config.RECENT_DAYS_FOR_UPTREND_REC_PRICE
        if trend > config.TREND_REL_FLAT_MAX
        else None
    )

    base_price, base_sales = compute_rec_price_default(
        sales, config.REC_PRICE_LOWER_Q_STABLE, recent_days
    )
    forecast_trend = metrics.get("trend_rel_30_down_forecast", trend)
    trend_factor = 1.0

    if trend < -config.TREND_REL_FLAT_MAX:
        trend_factor = trend_forecast_factor(forecast_trend)

    return base_price, base_sales, trend_factor


def compute_rec_price_downtrend(
    sales: List[Sale], metrics: Dict[str, float]
) -> Tuple[float, List[Sale]]:
    """
    Для стабильного нисходящего тренда:
      - берём базовую rec_price (как в default) по последним N дням,
      - умножаем на фактор падения согласно trend_rel_30 * (FORECAST_HORIZON_DAYS/30).
    """
    base_rec, base_sales = compute_rec_price_default(
        sales, config.REC_PRICE_LOWER_Q_STABLE
    )
    trend_rel_30 = metrics.get(
        "trend_rel_30_down_forecast", metrics["trend_rel_30"]
    )

    if trend_rel_30 >= 0:
        return base_rec, base_sales

    factor = trend_forecast_factor(trend_rel_30)

    return base_rec * factor, base_sales


def trend_forecast_factor(trend_rel_30: float) -> float:
    if trend_rel_30 >= 0:
        return 1.0

    factor = 1.0 + trend_rel_30 * (config.FORECAST_HORIZON_DAYS / 30.0)
    return max(factor, 0.0)


def compute_rec_price_wave(
    sales: List[Sale], metrics: Dict[str, float]
) -> Tuple[float, List[Sale], Optional[datetime]]:
    """
    Для волновых графиков:
      - берём только точки из валидных провалов (dips),
      - считаем по ним взвешенный квантиль REC_WAVE_Q.
      - если провалов нет (по нашим жёстким критериям) – fallback к default.
    """
    segments = find_valid_dip_segments(sales, metrics)
    if not segments:
        rec_price, rec_sales = compute_rec_price_default(
            sales, config.REC_PRICE_LOWER_Q_VOLATILE
        )
        return rec_price, rec_sales, None

    dip_sales: List[Sale] = []
    for seg in segments:
        dip_sales.extend(seg)

    prices = [s.price for s in dip_sales]
    weights = [s.amount for s in dip_sales]

    latest_dip_dt = max((s.dt for s in dip_sales), default=None)

    return weighted_quantile(prices, weights, config.REC_WAVE_Q), dip_sales, latest_dip_dt


def enforce_rec_price_sales_support(
    rec_price: float,
    support_sales: List[Sale],
    *,
    dip_based: bool = False,
    latest_dip_dt: Optional[datetime] = None,
    periods_override: Optional[List[Dict[str, Any]]] = None,
) -> float:
    """
    Проверяет поддержку rec_price на нескольких временных диапазонах. Для каждого
    периода окна строятся с шагом STEP_WINDOW_HOURS (он же ширина окна) в пределах
    последних HOURS часов (для второго периода – от HOURS второго до HOURS первого).
    Если число нарушений превышает MAX_ALLOWED_VIOLATIONS, rec_price понижается
    до уровня, удовлетворяющего всем оставшимся окнам.

    dip_based=True означает, что rec_price рассчитана по дипам. В этом случае
    используется полный список продаж в окнах, а если последний дип старше 24ч
    и настроено несколько периодов, проверка ведётся только на более длинных
    периодах.
    """
    if rec_price <= 0 or not support_sales:
        return rec_price

    sales_sorted = sorted(support_sales, key=lambda s: s.dt)
    overall_start = sales_sorted[0].dt
    last_dt = sales_sorted[-1].dt

    if overall_start >= last_dt:
        return rec_price

    periods_cfg = periods_override or config.REC_PRICE_SUPPORT_PERIODS
    if dip_based and latest_dip_dt is not None:
        age_since_dip = last_dt - latest_dip_dt
        if age_since_dip > timedelta(hours=24) and len(periods_cfg) > 1:
            periods_cfg = periods_cfg[1:]

    if not periods_cfg:
        return rec_price

    adjusted_price = rec_price
    period_end = last_dt

    for period_cfg in periods_cfg:
        hours_depth = timedelta(hours=period_cfg["HOURS"])
        step_window = timedelta(hours=period_cfg["STEP_WINDOW_HOURS"])
        min_share = period_cfg["MIN_SHARE"]
        allowed_violations = period_cfg["MAX_ALLOWED_VIOLATIONS"]
        min_window_volume = period_cfg.get(
            "MIN_WINDOW_VOLUME", config.REC_PRICE_SUPPORT_MIN_WINDOW_VOLUME
        )
        period_rec_price = adjusted_price
        period_violations: List[Dict[str, Any]] = []

        period_start = max(overall_start, last_dt - hours_depth)
        if period_end <= period_start:
            period_end = period_start
            continue

        period_sales = [
            s for s in sales_sorted if period_start <= s.dt <= period_end
        ]
        if not period_sales:
            period_end = period_start
            continue

        violating_candidates: List[float] = []
        t = period_start
        while t < period_end:
            window_end_ts = min(t + step_window, period_end)
            window_sales = [s for s in period_sales if t <= s.dt <= window_end_ts]
            total_vol = sum(s.amount for s in window_sales)

            if total_vol < min_window_volume:
                t += step_window
                continue

            required_vol = total_vol * min_share
            support_vol = sum(
                s.amount for s in window_sales if s.price >= period_rec_price
            )
            acc = 0
            candidate_price = 0.0
            for sale in sorted(window_sales, key=lambda s: s.price, reverse=True):
                acc += sale.amount
                candidate_price = sale.price
                if acc >= required_vol:
                    break

            if candidate_price < period_rec_price:
                violating_candidates.append(candidate_price)
                period_violations.append(
                    {
                        "start": t,
                        "end": window_end_ts,
                        "candidate_price": candidate_price,
                        "total_vol": total_vol,
                        "required_vol": required_vol,
                        "support_vol": support_vol,
                        "min_share": min_share,
                        "rec_price": period_rec_price,
                    }
                )

            t += step_window

        if period_violations:
            print(
                "[REC_SUPPORT] Нарушения поддержки продаж: период последних "
                f"{period_cfg['HOURS']}ч, rec_price={period_rec_price:.2f}, "
                f"step={period_cfg['STEP_WINDOW_HOURS']}ч"
            )
            for v in period_violations:
                print(
                    "    окно "
                    f"{v['start'].isoformat()} – {v['end'].isoformat()}: "
                    f"candidate_price={v['candidate_price']:.2f}, "
                    f"объём={v['total_vol']}, нужно>= {v['required_vol']:.2f} "
                    f"(объём>=rec_price: {v['support_vol']:.2f}) "
                    f"(min_share={v['min_share']:.2f}, rec_price={v['rec_price']:.2f})"
                )

        if len(violating_candidates) > allowed_violations:
            violating_candidates.sort()
            adjusted_price = min(adjusted_price, violating_candidates[allowed_violations])

        period_end = period_start

    return adjusted_price


# ---------- Прочие вычисления ----------

def compute_avg_sales_last_two_weeks(sales: List[Sale]) -> float:
    """
    Кол-во продаж:
      - сумма amount за последние 7 дней (week1)
      - сумма amount за предыдущие 7 дней (week2)
      - возвращаем среднее (week1 + week2) / 2
    """
    if not sales:
        return 0.0

    sales_sorted = sorted(sales, key=lambda s: s.dt)
    last_dt = sales_sorted[-1].dt
    w1_start = last_dt - timedelta(days=7)
    w2_start = last_dt - timedelta(days=14)

    week1 = sum(s.amount for s in sales_sorted if s.dt >= w1_start)
    week2 = sum(s.amount for s in sales_sorted if w2_start <= s.dt < w1_start)

    if week1 == 0 and week2 == 0:
        return 0.0

    return (week1 + week2) / 2.0


# ---------- Boost / Crash detection ----------

def detect_boost_crash(sales: List[Sale]) -> Dict[str, Any]:
    """
    Делит 30-дневные продажи на старую и свежую часть.
    Смотрит на отношение base_recent / base_old:
      - >= BOOST_MIN_RATIO  → boost
      - <= CRASH_MIN_RATIO  → crash
      - иначе               → none
    """
    result = {"kind": "none"}

    if not sales:
        return result

    sales_sorted = sorted(sales, key=lambda s: s.dt)
    last_dt = sales_sorted[-1].dt
    split_dt = last_dt - timedelta(days=config.BOOST_RECENT_DAYS)

    old_sales = [s for s in sales_sorted if s.dt < split_dt]
    recent_sales = [s for s in sales_sorted if s.dt >= split_dt]

    if not old_sales or not recent_sales:
        return result

    vol_old = sum(s.amount for s in old_sales)
    vol_recent = sum(s.amount for s in recent_sales)

    if vol_old < config.BOOST_MIN_OLD_VOLUME or vol_recent < config.BOOST_MIN_RECENT_VOLUME:
        return result

    metrics_old = compute_basic_metrics(old_sales)
    metrics_recent = compute_basic_metrics(recent_sales)

    base_old = metrics_old["base_price"]
    base_recent = metrics_recent["base_price"]

    if base_old <= 0 or base_recent <= 0:
        return result

    ratio = base_recent / base_old

    if ratio >= config.BOOST_MIN_RATIO:
        return {
            "kind": "boost",
            "ratio": ratio,
            "old_sales": old_sales,
            "recent_sales": recent_sales,
            "metrics_old": metrics_old,
            "metrics_recent": metrics_recent,
        }
    elif ratio <= config.CRASH_MIN_RATIO:
        return {
            "kind": "crash",
            "ratio": ratio,
            "old_sales": old_sales,
            "recent_sales": recent_sales,
            "metrics_old": metrics_old,
            "metrics_recent": metrics_recent,
        }

    return result


# ---------- Форма графика / классификация ----------

def classify_shape_basic(
    sales: List[Sale],
    metrics: Dict[str, float],
    dips: Dict[str, float],
    allow_wave: bool = True,
) -> Dict[str, Any]:
    """
    Базовая классификация графика БЕЗ учёта boost/crash.
    Типы:
      - stable_flat, tier=1
      - stable_up,   tier=2
      - stable_down, tier=3
      - wave,        tier=5
      - old_dip_dips, tier=5
      - volatile,    tier=4
    Возможен также status="blacklist" (слишком много дипов или сильный спад).
    """
    base = metrics["base_price"]
    trend = metrics["trend_rel_30"]
    cv = metrics["cv_price"]
    wave_amp = metrics.get("wave_amp_detrended", 0.0)
    wave_amp_raw = metrics.get("wave_amp_raw", wave_amp)

    old_dips = dips.get("old_dips_days", 0.0)
    recent_dips = dips.get("recent_dips_days", 0.0)
    total_dip_days = old_dips + recent_dips

    wave_info = f"wave_amp(det)={wave_amp:.3f}, raw={wave_amp_raw:.3f}"

    def _ok_result(
        graph_type: str,
        tier: int,
        rec_price: float,
        rec_price_sales: List[Sale],
        reason: str,
        *,
        rec_price_support_sales: Optional[List[Sale]] = None,
        rec_price_from_dip: bool = False,
        latest_dip_dt: Optional[datetime] = None,
        post_support_trend_factor: float = 1.0,
    ) -> Dict[str, Any]:
        return {
            "status": "ok",
            "graph_type": graph_type,
            "tier": tier,
            "rec_price": rec_price,
            "rec_price_sales": rec_price_sales,
            "rec_price_support_sales": rec_price_support_sales
            if rec_price_support_sales is not None
            else rec_price_sales,
            "rec_price_from_dip": rec_price_from_dip,
            "latest_dip_dt": latest_dip_dt,
            "post_support_trend_factor": post_support_trend_factor,
            "reason": reason,
        }

    # 1) Жёсткие отсечки по провалам
    if old_dips > config.DIP_MAX_OLD_DAYS:
        dip_rec_price, dip_sales, latest_dip_dt = compute_rec_price_wave(sales, metrics)
        stable_rec_price, stable_sales, stable_trend_factor = compute_stable_like_rec_price(
            sales, metrics
        )

        dip_supported_price = enforce_rec_price_sales_support(
            dip_rec_price,
            dip_sales,
            dip_based=True,
            latest_dip_dt=latest_dip_dt,
        )
        stable_supported_price = enforce_rec_price_sales_support(
            stable_rec_price,
            stable_sales,
        )

        use_dip_price = dip_supported_price <= stable_supported_price
        chosen_supported_price = (
            dip_supported_price if use_dip_price else stable_supported_price
        )
        chosen_sales = dip_sales if use_dip_price else stable_sales
        chosen_support_sales = dip_sales if use_dip_price else stable_sales
        post_trend_factor = 1.0 if use_dip_price else stable_trend_factor
        final_rec_price = chosen_supported_price * post_trend_factor

        print(
            f"[REC_PRICE] old_dip_dips: dip_supported={dip_supported_price:.4f}, "
            f"stable_supported={stable_supported_price:.4f}, "
            f"chosen={'dip' if use_dip_price else 'stable'}, "
            f"post_trend_factor={post_trend_factor:.4f}, final_rec_price={final_rec_price:.4f}"
        )

        if trend < config.MAX_DOWN_TREND_REL or trend > config.MAX_UP_TREND_REL:
            return {
                "status": "blacklist",
                "reason": (
                    "old_dip_dips_trend_out_of_range "
                    f"({trend*100:.1f}% not in "
                    f"[{config.MAX_DOWN_TREND_REL*100:.1f}%; {config.MAX_UP_TREND_REL*100:.1f}%])"
                ),
            }

        return _ok_result(
            graph_type="old_dip_dips",
            tier=5,
            rec_price=final_rec_price,
            rec_price_sales=chosen_sales,
            rec_price_support_sales=chosen_support_sales,
            rec_price_from_dip=use_dip_price,
            latest_dip_dt=latest_dip_dt,
            post_support_trend_factor=post_trend_factor,
            reason=(
                f"old_dips_excess ({old_dips:.2f} days); dip_supported={dip_supported_price:.4f}; "
                f"stable_supported={stable_supported_price:.4f}; "
                f"chosen={'dip' if use_dip_price else 'stable'}; "
                f"post_trend_factor={post_trend_factor:.3f}; {wave_info}"
            ),
        )

    if recent_dips > config.DIP_MAX_RECENT_DAYS:
        return {
            "status": "blacklist",
            "reason": f"recent_deep_dips ({recent_dips:.2f} days)",
        }

    # 2) Сильный нисходящий тренд – сразу блэклист (резкий слив)
    if trend < config.MAX_DOWN_TREND_REL:
        return {
            "status": "blacklist",
            "reason": f"strong_downtrend ({trend*100:.1f}%)",
        }

    # 2a) Сильный восходящий тренд – блэклист (если это не буст)
    if trend > config.MAX_UP_TREND_REL:
        return {
            "status": "blacklist",
            "reason": f"strong_uptrend ({trend*100:.1f}%)",
        }

    # shape_ok = спокойный график по разбросу
    shape_ok = (cv <= config.STABLE_MAX_CV and wave_amp <= config.STABLE_MAX_WAVE_AMP)

    # 3) stable_flat (tier 1): тренд -5..+5%, форма графика спокойная
    if abs(trend) <= config.TREND_REL_FLAT_MAX and shape_ok:
        rec_price, rec_price_sales = compute_rec_price_default(
            sales, config.REC_PRICE_LOWER_Q_STABLE
        )
        return _ok_result(
            graph_type="stable_flat",
            tier=1,
            rec_price=rec_price,
            rec_price_sales=rec_price_sales,
            reason=(
                f"stable_flat; trend={trend*100:.1f}%, cv={cv:.3f}, {wave_info}"
            ),
        )

    # 4) stable_up (tier 2): тренд вверх, не слишком резкий, форма спокойная
    if trend > config.TREND_REL_FLAT_MAX and trend <= config.MAX_UP_TREND_REL and shape_ok:
        rec_price, rec_price_sales = compute_rec_price_default(
            sales,
            config.REC_PRICE_LOWER_Q_STABLE,
            config.RECENT_DAYS_FOR_UPTREND_REC_PRICE,
        )
        return _ok_result(
            graph_type="stable_up",
            tier=2,
            rec_price=rec_price,
            rec_price_sales=rec_price_sales,
            reason=(
                f"stable_up; trend={trend*100:.1f}%, cv={cv:.3f}, {wave_info}"
            ),
        )

    # 5) stable_down (tier 3): тренд вниз, умеренный, форма спокойная
    if trend < -config.TREND_REL_FLAT_MAX and trend >= config.MAX_DOWN_TREND_REL and shape_ok:
        rec_price, rec_price_sales = compute_rec_price_default(
            sales, config.REC_PRICE_LOWER_Q_STABLE
        )
        trend_forecast = metrics.get("trend_rel_30_down_forecast", trend)
        factor = 1.0
        if trend_forecast < 0:
            factor = trend_forecast_factor(trend_forecast)
        return _ok_result(
            graph_type="stable_down",
            tier=3,
            rec_price=rec_price,
            rec_price_sales=rec_price_sales,
            post_support_trend_factor=factor,
            reason=(
                f"stable_down; trend={trend*100:.1f}%, forecast_trend={trend_forecast*100:.1f}%, cv={cv:.3f}, {wave_info}, "
                f"trend_factor={factor:.3f}"
            ),
        )

    # 6) wave (tier 4): большая амплитуда + есть существенные провалы
    if (
        allow_wave
        and wave_amp >= config.WAVE_MIN_AMP
        and total_dip_days >= config.WAVE_MIN_DIP_DAYS
    ):
        dip_rec_price, rec_price_sales, latest_dip_dt = compute_rec_price_wave(
            sales, metrics
        )
        stable_rec_price, stable_sales, stable_trend_factor = compute_stable_like_rec_price(
            sales, metrics
        )

        dip_final_price = dip_rec_price
        stable_final_price = stable_rec_price * stable_trend_factor

        use_dip_price = dip_final_price <= stable_final_price
        chosen_price = dip_rec_price if use_dip_price else stable_rec_price
        chosen_sales = rec_price_sales if use_dip_price else stable_sales
        post_trend_factor = 1.0 if use_dip_price else stable_trend_factor

        print(
            f"[REC_PRICE] wave: dip_based={dip_final_price:.4f}, "
            f"stable_like={stable_final_price:.4f}, chosen={'dip' if use_dip_price else 'stable'}"
        )

        return _ok_result(
            graph_type="wave",
            tier=5,
            rec_price=chosen_price,
            rec_price_sales=chosen_sales,
            rec_price_support_sales=sales if use_dip_price else stable_sales,
            rec_price_from_dip=use_dip_price,
            latest_dip_dt=latest_dip_dt,
            post_support_trend_factor=post_trend_factor,
            reason=(
                f"wave; trend={trend*100:.1f}%, cv={cv:.3f}, {wave_info}, dip_days={total_dip_days:.2f}; "
                f"dip_rec={dip_final_price:.4f}; stable_like={stable_final_price:.4f}; "
                f"chosen={'dip' if use_dip_price else 'stable'}"
            ),
        )

    # 7) всё остальное – нестабильные / сложные графики (tier 4)
    recent_days = (
        config.RECENT_DAYS_FOR_UPTREND_REC_PRICE
        if trend > config.TREND_REL_FLAT_MAX
        else None
    )
    rec_price, rec_price_sales = compute_rec_price_default(
        sales, config.REC_PRICE_LOWER_Q_VOLATILE, recent_days
    )
    graph_type = "volatile"
    trend_factor = 1.0
    if trend > config.TREND_REL_FLAT_MAX:
        graph_type = "volatile_up_trend"
        if trend < 0:
            trend_factor = trend_forecast_factor(trend)
    elif trend < -config.TREND_REL_FLAT_MAX:
        graph_type = "volatile_down_trend"
        trend_forecast = metrics.get("trend_rel_30_down_forecast", trend)
        if trend_forecast < 0:
            trend_factor = trend_forecast_factor(trend_forecast)

    return _ok_result(
        graph_type=graph_type,
        tier=4,
        rec_price=rec_price,
        rec_price_sales=rec_price_sales,
        post_support_trend_factor=trend_factor,
        reason=(
            f"{graph_type}; trend={trend*100:.1f}%, cv={cv:.3f}, {wave_info}, "
            f"trend_factor={trend_factor:.3f}"
        ),
    )


def classify_shape_and_rec_price(
    sales: List[Sale],
    metrics: Dict[str, float],
    dips: Dict[str, float],
) -> Dict[str, Any]:
    """
    Полная классификация с учётом:
      - crash (резкий слив и удержание цены внизу) → блэклист
      - boost (резкий рост и удержание сверху) → анализ старой части
      - stable_flat / stable_up / stable_down
      - wave / volatile
    """
    # 0) Пытаемся обнаружить boost/crash
    bc = detect_boost_crash(sales)
    kind = bc.get("kind", "none")

    if kind == "crash":
        ratio = bc["ratio"]
        return {
            "status": "blacklist",
            "reason": f"crash_detected (base_recent/base_old={ratio:.2f})",
        }

    if kind == "boost":
        ratio = bc["ratio"]
        old_sales: List[Sale] = bc["old_sales"]
        metrics_old = bc["metrics_old"]
        dips_old = compute_price_dips(old_sales, metrics_old)

        # Анализируем старую часть: она должна быть стабильной (tier 1–3).
        base_res = classify_shape_basic(old_sales, metrics_old, dips_old, allow_wave=False)

        if base_res["status"] == "blacklist":
            return {
                "status": "blacklist",
                "reason": f"boost_unstable_old ({base_res.get('reason')})",
            }

        if not base_res["graph_type"].startswith("stable_"):
            # старая часть тоже волатильная/волновая → блэклист
            return {
                "status": "blacklist",
                "reason": f"boost_old_not_stable (graph_type={base_res['graph_type']})",
            }

        rec_price = base_res["rec_price"]
        rec_price_sales = base_res.get("rec_price_sales", [])
        return {
            "status": "ok",
            "graph_type": "boost",
            "tier": 4,
            "rec_price": rec_price,
            "rec_price_sales": rec_price_sales,
            "rec_price_support_sales": base_res.get(
                "rec_price_support_sales", rec_price_sales
            ),
            "rec_price_from_dip": base_res.get("rec_price_from_dip", False),
            "latest_dip_dt": base_res.get("latest_dip_dt"),
            "post_support_trend_factor": base_res.get(
                "post_support_trend_factor", 1.0
            ),
            "reason": (
                f"boost_detected (ratio={ratio:.2f}); base_part={base_res['graph_type']}, "
                f"base_reason={base_res['reason']}"
            ),
        }

    # 1) Если буста/краша нет – обычная базовая классификация для всего графика
    return classify_shape_basic(sales, metrics, dips, allow_wave=True)


# ---------- Парсинг HTML Steam ----------

LINE1_REGEX = re.compile(r"var\s+line1\s*=\s*(\[\[.*?\]\]);", re.DOTALL)


def parse_sales_from_html(item_name: str, html_text: str) -> List[Sale]:
    """
    Ищет в HTML переменную line1 и парсит её в список Sale за последние 30 дней.
    Формат line1: [["Oct 29 2020 01: +0", 7.123, "44"], ...]
    """
    m = LINE1_REGEX.search(html_text)
    if not m:
        raise ValueError("Не удалось найти line1 в HTML")

    data_str = m.group(1)
    try:
        data = json.loads(data_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON parse error in line1: {e}")

    sales: List[Sale] = []
    for entry in data:
        if not isinstance(entry, list) or len(entry) < 3:
            continue
        date_str = entry[0]
        price = float(entry[1])
        amount = int(entry[2])

        # date_str вида "Oct 29 2020 01: +0" – отрежем хвост ":+0"
        if ": +" in date_str:
            date_part = date_str.split(": +", 1)[0]
        else:
            parts = date_str.split()
            if len(parts) >= 4:
                month, day, year, hour_part = parts[:4]
                hour = hour_part.replace(":", "")
                date_part = f"{month} {day} {year} {hour}"
            else:
                date_part = date_str

        try:
            dt = datetime.strptime(date_part, "%b %d %Y %H")
        except ValueError as e:
            raise ValueError(f"Date parse error '{date_str}': {e}")

        sales.append(Sale(dt=dt, price=price, amount=amount))

    if not sales:
        raise ValueError("Не удалось распарсить ни одной продажи из line1")

    sales.sort(key=lambda s: s.dt)
    last_dt = sales[-1].dt
    cutoff = last_dt - timedelta(days=30)
    sales_30d = [s for s in sales if s.dt >= cutoff]

    print(f"[DOWNLOAD] Успешно спарсили {len(sales_30d)} записей продаж за последние 30 дней.")
    dump_sales_debug(item_name, sales_30d)

    return sales_30d


def dump_sales_debug(item_name: str, sales: List[Sale]) -> None:
    """
    Записывает все продажи в sell_parsing/<item>.txt в формате:
    Цена   кол-во   Время   Дата
    """
    fn = os.path.join(config.SELL_PARSING_DIR, f"{safe_filename(item_name)}.txt")
    sales_sorted = sorted(sales, key=lambda s: s.dt)
    ranges = build_three_day_ranges(sales_sorted)
    with open(fn, "w", encoding="utf-8") as f:
        metrics = compute_basic_metrics(sales)
        histogram = compute_old_dip_histogram(sales, metrics)
        last_dt = max(sales, key=lambda s: s.dt).dt

        bin_size_days = 3.0

        f.write("old_dip_histogram (days_from_last_sale; bin=3d)\n")
        for idx, days in enumerate(histogram):
            left = idx * bin_size_days
            right = left + bin_size_days
            bin_start_dt = last_dt - timedelta(days=right)
            bin_end_dt = last_dt - timedelta(days=left)
            f.write(
                f"{left:02.0f}-{right:02.0f}d\t{days:.3f}\t"
                f"{bin_start_dt:%Y-%m-%d %H:%M} -> {bin_end_dt:%Y-%m-%d %H:%M}\n"
            )
        f.write("\n")

        f.write("price\tamount\ttime\tdate\n")
        for s in sales_sorted:
            f.write(
                f"{s.price:.6f}\t{s.amount}\t"
                f"{s.dt.strftime('%H:%M')}\t{s.dt.strftime('%Y-%m-%d')}\n"
            )

        if ranges:
            f.write("\n3-day ranges (UTC):\n")
            for idx, (start_dt, end_dt) in enumerate(ranges, start=1):
                f.write(
                    f"{idx:02d}: {start_dt.strftime('%Y-%m-%d %H:%M')} — "
                    f"{end_dt.strftime('%Y-%m-%d %H:%M')}\n"
                )


def build_three_day_ranges(sales: List[Sale]) -> List[Tuple[datetime, datetime]]:
    """
    Формирует список непрерывных 3-дневных диапазонов от самой ранней продажи
    до самой поздней. Каждая пара содержит начало и конец диапазона в UTC.
    """
    if not sales:
        return []

    earliest = sales[0].dt
    last = sales[-1].dt

    ranges: List[Tuple[datetime, datetime]] = []
    current_end = last

    while True:
        current_start = current_end - timedelta(days=3)
        ranges.append((current_start, current_end))
        if current_start <= earliest:
            break
        current_end = current_start

    return list(reversed(ranges))


# ---------- Работа с прокси и скачивание HTML ----------
# (эта часть не менялась, оставляю как в предыдущей версии)

def get_all_proxies() -> List[sqlite3.Row]:
    conn = get_proxy_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM proxies WHERE disabled = 0 ORDER BY id ASC")
    rows = cur.fetchall()
    conn.close()
    return rows


def get_last_proxy_position() -> int:
    conn = get_proxy_conn()
    cur = conn.cursor()
    cur.execute("SELECT last_position FROM proxy_state WHERE id = 1")
    row = cur.fetchone()
    conn.close()
    if row is None or row["last_position"] is None:
        return -1
    return int(row["last_position"])


def set_last_proxy_position(position: int) -> None:
    conn = get_proxy_conn()
    cur = conn.cursor()
    cur.execute("UPDATE proxy_state SET last_position = ? WHERE id = 1", (position,))
    conn.commit()
    conn.close()


def update_proxy_row(
    proxy_id: int,
    *,
    last_used_ts: Optional[float] = None,
    rest_until_ts: Optional[float] = None,
    fail_stage: Optional[int] = None,
    fallback_fail_count: Optional[int] = None,
    disabled: Optional[int] = None,
) -> None:
    conn = get_proxy_conn()
    cur = conn.cursor()

    sets = []
    params: List[Any] = []
    if last_used_ts is not None:
        sets.append("last_used_ts = ?")
        params.append(last_used_ts)
    if rest_until_ts is not None:
        sets.append("rest_until_ts = ?")
        params.append(rest_until_ts)
    if fail_stage is not None:
        sets.append("fail_stage = ?")
        params.append(fail_stage)
    if fallback_fail_count is not None:
        sets.append("fallback_fail_count = ?")
        params.append(fallback_fail_count)
    if disabled is not None:
        sets.append("disabled = ?")
        params.append(disabled)

    if not sets:
        conn.close()
        return

    params.append(proxy_id)
    sql = "UPDATE proxies SET " + ", ".join(sets) + " WHERE id = ?"
    cur.execute(sql, params)
    conn.commit()
    conn.close()


def build_requests_proxy(address: str) -> Dict[str, str]:
    """
    Преобразует строку формата ip:port@password:user
    в словарь для requests.
    """
    host_port, auth = address.split("@", 1)
    user, password = auth.split(":", 1)
    proxy_auth = f"{user}:{password}@{host_port}"
    return {
        "http": f"http://{proxy_auth}",
        "https": f"http://{proxy_auth}",
    }


def direct_is_resting(now_ts: Optional[float] = None) -> bool:
    if now_ts is None:
        now_ts = time.time()
    return direct_ip_state["rest_until_ts"] > now_ts


def send_direct_to_rest(now_ts: Optional[float] = None) -> None:
    """Отправляет прямой IP на отдых по той же схеме, что и прокси."""
    if now_ts is None:
        now_ts = time.time()

    if direct_ip_state["fail_stage"] == 0:
        rest_until = now_ts + config.REST_PROXY1
        stage = 1
        print(
            f"[PROXY] Отправляем прямой IP на первый отдых на {config.REST_PROXY1} сек."
        )
    else:
        rest_until = now_ts + config.REST_PROXY2
        stage = 2
        print(
            f"[PROXY] Отправляем прямой IP на второй отдых на {config.REST_PROXY2} сек."
        )

    direct_ip_state["rest_until_ts"] = rest_until
    direct_ip_state["fail_stage"] = stage


def fetch_html_direct(url: str) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0 Safari/537.36",
    }
    resp = requests.get(url, headers=headers, timeout=config.HTTP_TIMEOUT)
    resp.raise_for_status()
    return resp.text


def fetch_html_with_proxies(url: str, item_name: str) -> str:
    """
    Скачивает HTML с учётом настроек PROXY_SELECT и логики отдыха прокси.
    """
    ensure_directories()

    last_error_html: Optional[str] = None

    if config.PROXY_SELECT == 0:
        attempt = 0
        while attempt < config.MAX_HTML_RETRIES:
            now_ts = time.time()
            if direct_is_resting(now_ts):
                remain = int(direct_ip_state["rest_until_ts"] - now_ts)
                print(f"[PROXY] Прямой IP ещё отдыхает {remain} сек.")
                time.sleep(max(1, min(remain, config.DELAY_DOWNLOAD_ERROR)))
                continue

            attempt += 1
            try:
                html = fetch_html_direct(url)
                temp_path = os.path.join(
                    config.HTML_TEMP_DIR,
                    f"{safe_filename(item_name)}.html",
                )
                with open(temp_path, "w", encoding="utf-8") as f:
                    f.write(html)
                return html
            except requests.HTTPError as e:
                resp = e.response
                if resp is not None and resp.status_code == 429:
                    last_error_html = resp.text
                    send_direct_to_rest(now_ts)
                    continue
                print(f"[DOWNLOAD] Ошибка прямого скачивания (попытка {attempt}): {e}")
                time.sleep(config.DELAY_DOWNLOAD_ERROR)
            except requests.RequestException as e:
                print(f"[DOWNLOAD] Ошибка прямого скачивания (попытка {attempt}): {e}")
                time.sleep(config.DELAY_DOWNLOAD_ERROR)
        raise RuntimeError("Не удалось скачать страницу через прямой IP после нескольких попыток.")

    proxies_rows = get_all_proxies()
    if not proxies_rows:
        print("[PROXY] Нет доступных прокси в БД, пробуем сразу прямой IP.")
        return fetch_html_direct(url)

    direct_entry = None
    if config.PROXY_SELECT == 2:
        direct_entry = {
            "id": None,
            "address": "DIRECT",
            "last_used_ts": 0.0,
            "rest_until_ts": direct_ip_state["rest_until_ts"],
            "fail_stage": direct_ip_state["fail_stage"],
            "fallback_fail_count": 0,
            "disabled": 0,
        }

    proxy_cycle = list(proxies_rows)
    if direct_entry is not None:
        proxy_cycle.append(direct_entry)

    cycle_len = len(proxy_cycle)
    if cycle_len == 0:
        return fetch_html_direct(url)

    last_position = get_last_proxy_position()
    if last_position < -1:
        last_position = -1
    start_index = (last_position + 1) % cycle_len
    attempt = 0
    last_error_html: Optional[str] = None

    while attempt < config.MAX_HTML_RETRIES:
        attempt += 1
        print(f"[DOWNLOAD] Попытка #{attempt} скачивания HTML...")

        for offset in range(cycle_len):
            idx = (start_index + offset) % cycle_len
            row = proxy_cycle[idx]

            if row["address"] == "DIRECT":
                use_direct = True
                proxy_id = None
                rest_until_ts = direct_ip_state["rest_until_ts"]
                last_used_ts = 0.0
                fail_stage = direct_ip_state["fail_stage"]
                fallback_fail_count = 0
            else:
                use_direct = False
                proxy_id = row["id"]
                rest_until_ts = row["rest_until_ts"] or 0.0
                last_used_ts = row["last_used_ts"] or 0.0
                fail_stage = row["fail_stage"] or 0
                fallback_fail_count = row["fallback_fail_count"] or 0

            now_ts = time.time()

            if rest_until_ts > now_ts:
                remain = int(rest_until_ts - now_ts)
                label = "прямой IP" if use_direct else f"прокси {row['address']}"
                print(f"[PROXY] {label} ещё отдыхает {remain} сек.")
                continue

            if not config.DELAY_OFF and not use_direct and last_used_ts > 0:
                delta = now_ts - last_used_ts
                if delta < config.DELAY_HTML:
                    wait_sec = int(config.DELAY_HTML - delta)
                    print(
                        f"[PROXY] Высокая нагрузка на прокси {row['address']}, "
                        f"ждём {wait_sec} сек."
                    )
                    time.sleep(max(wait_sec, 1))

            try:
                if use_direct:
                    proxies = None
                    print("[PROXY] Используем прямой IP.")
                else:
                    proxies = build_requests_proxy(row["address"])
                    print(f"[PROXY] Используем прокси {row['address']}.")

                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                                  "Chrome/120.0 Safari/537.36",
                }
                resp = requests.get(
                    url,
                    headers=headers,
                    timeout=config.HTTP_TIMEOUT,
                    proxies=proxies,
                )
                if resp.status_code == 429:
                    text = resp.text
                    last_error_html = text
                    print(f"[PROXY] 429 Too Many Requests от {row['address'] if not use_direct else 'DIRECT'}.")

                    if use_direct:
                        send_direct_to_rest()
                    elif proxy_id is not None:
                        now_ts = time.time()
                        if fail_stage == 0:
                            rest_until = now_ts + config.REST_PROXY1
                            print(
                                f"[PROXY] Отправляем прокси {row['address']} "
                                f"на первый отдых на {config.REST_PROXY1} сек."
                            )
                            update_proxy_row(
                                proxy_id,
                                rest_until_ts=rest_until,
                                fail_stage=1,
                            )
                        else:
                            rest_until = now_ts + config.REST_PROXY2
                            print(
                                f"[PROXY] Отправляем прокси {row['address']} "
                                f"на второй отдых на {config.REST_PROXY2} сек."
                            )
                            update_proxy_row(
                                proxy_id,
                                rest_until_ts=rest_until,
                                fail_stage=2,
                            )
                    continue

                resp.raise_for_status()
                html = resp.text

                if not use_direct and proxy_id is not None:
                    update_proxy_row(proxy_id, last_used_ts=time.time())

                temp_path = os.path.join(
                    config.HTML_TEMP_DIR,
                    f"{safe_filename(item_name)}.html",
                )
                with open(temp_path, "w", encoding="utf-8") as f:
                    f.write(html)

                set_last_proxy_position(idx)
                return html

            except requests.RequestException as e:
                print(
                    f"[DOWNLOAD] Сетевая ошибка через "
                    f"{row['address'] if not use_direct else 'DIRECT'}: {e}"
                )
                if not use_direct and proxy_id is not None:
                    try:
                        if direct_is_resting():
                            remain = int(direct_ip_state["rest_until_ts"] - time.time())
                            print(
                                f"[DOWNLOAD] Прямой IP отдыхает, ждём {remain} сек и пробуем другие прокси."
                            )
                            continue
                        print("[DOWNLOAD] Пытаемся скачать через прямой IP после ошибки прокси...")
                        html = fetch_html_direct(url)
                        now_ts = time.time()
                        rest_until = now_ts + config.REST_PROXY1
                        fallback_fail_count += 1
                        print(
                            f"[PROXY] Прокси {row['address']} отправляется на отдых "
                            f"на {config.REST_PROXY1} сек. "
                            f"(fallback_fail_count={fallback_fail_count})"
                        )
                        disabled_flag = 0
                        if fallback_fail_count >= config.MAX_FALLBACK_FAILS:
                            disabled_flag = 1
                            print(
                                f"[PROXY] Прокси {row['address']} "
                                f"навсегда отключен (слишком много сбоев)."
                            )
                        update_proxy_row(
                            proxy_id,
                            rest_until_ts=rest_until,
                            fallback_fail_count=fallback_fail_count,
                            disabled=disabled_flag,
                        )

                        temp_path = os.path.join(
                            config.HTML_TEMP_DIR,
                            f"{safe_filename(item_name)}.html",
                        )
                        with open(temp_path, "w", encoding="utf-8") as f:
                            f.write(html)

                        set_last_proxy_position(idx)
                        return html
                    except requests.HTTPError as e2:
                        resp = e2.response
                        if resp is not None and resp.status_code == 429:
                            print(
                                "[DOWNLOAD] Прямой IP вернул 429 при проверке соединения; "
                                "считаем, что соединение есть."
                            )
                            send_direct_to_rest()
                            now_ts = time.time()
                            rest_until = now_ts + config.REST_PROXY1
                            fallback_fail_count += 1
                            disabled_flag = 0
                            if fallback_fail_count >= config.MAX_FALLBACK_FAILS:
                                disabled_flag = 1
                                print(
                                    f"[PROXY] Прокси {row['address']} "
                                    f"навсегда отключен (слишком много сбоев)."
                                )
                            update_proxy_row(
                                proxy_id,
                                rest_until_ts=rest_until,
                                fallback_fail_count=fallback_fail_count,
                                disabled=disabled_flag,
                            )
                            continue
                        print(
                            f"[DOWNLOAD] Не удалось скачать через прямой IP после ошибки прокси: {e2}"
                        )
                        time.sleep(config.DELAY_DOWNLOAD_ERROR)
                        continue
                    except requests.RequestException as e2:
                        print(
                            f"[DOWNLOAD] Не удалось скачать через прямой IP после ошибки прокси: {e2}"
                        )
                        time.sleep(config.DELAY_DOWNLOAD_ERROR)
                        continue
                else:
                    time.sleep(config.DELAY_DOWNLOAD_ERROR)
                    continue

        start_index = (start_index + 1) % cycle_len

    if last_error_html:
        temp_path = os.path.join(
            config.HTML_FAILED_DIR,
            f"{safe_filename(item_name)}_last_error.html",
        )
        with open(temp_path, "w", encoding="utf-8") as f:
            f.write(last_error_html)

    raise RuntimeError("Не удалось скачать страницу через прокси/прямой IP после нескольких попыток.")


# ---------- Основная функция парсинга и анализа ----------

def parsing_steam_sales(url: str) -> Dict[str, Any]:
    """
    Главная функция.
    1) Проверяет ссылку.
    2) Определяет itemName и game_code.
    3) Пытается использовать кэш из steam_items.
    4) Проверяет блэклист.
    5) Скачивает HTML (с прокси или без).
    6) Парсит продажи, считает метрики, дипы, тип графика, рек. цену.
    7) Пишет результат в БД и складывает HTML в нужную папку.
    """
    ensure_directories()

    url = url.strip()
    if not (url.startswith("http://") or url.startswith("https://")):
        return {
            "status": "invalid_link",
            "message": "not correct link",
            "item_name": None,
        }

    if "steamcommunity.com/market/listings/" not in url:
        return {
            "status": "invalid_link",
            "message": "not correct link",
            "item_name": None,
        }

    game_code = None
    item_encoded = None

    m_730 = re.search(r"listings/730/([^/?#]+)", url)
    m_570 = re.search(r"listings/570/([^/?#]+)", url)

    if m_730:
        game_code = 730
        item_encoded = m_730.group(1)
    elif m_570:
        game_code = 570
        item_encoded = m_570.group(1)
    else:
        return {
            "status": "invalid_link",
            "message": "not correct link (dont found game code)",
            "item_name": None,
        }

    item_name = unquote(item_encoded)

    if game_code == 570:
        return {
            "status": "dota_soon",
            "message": "dota soon",
            "item_name": item_name,
        }

    # --- Проверяем кэш ---
    row = get_cached_item(item_name)
    if row is not None and row["rec_price"] is not None:
        updated_at = row["updated_at"]
        try:
            dt_updated = datetime.fromisoformat(updated_at)
        except Exception:
            dt_updated = None

        if dt_updated is not None:
            age_hours = (datetime.utcnow() - dt_updated).total_seconds() / 3600.0
            if age_hours < config.ACTUAL_HOURS:
                print(
                    f"[CACHE] Используем кэш для {item_name}: "
                    f"rec_price={row['rec_price']}, avg_sales={row['avg_sales']}, "
                    f"tier={row['tier']}, type={row['graph_type']} "
                    f"(возраст {age_hours:.2f} ч)"
                )
                return {
                    "status": "ok",
                    "item_name": item_name,
                    "rec_price": float(row["rec_price"]),
                    "avg_sales": float(row["avg_sales"]),
                    "tier": int(row["tier"]),
                    "graph_type": row["graph_type"],
                    "message": "from_cache",
                }

    # --- Проверяем блэклист ---
    bl_info = get_active_blacklist_entry(item_name)
    if bl_info is not None:
        entry = bl_info["entry"]
        expires_at_str = entry.get("expires_at") if isinstance(entry, dict) else entry["expires_at"]
        reason = entry.get("reason") if isinstance(entry, dict) else entry["reason"]
        print(
            f"[BLACKLIST] {item_name} в блэклисте до {expires_at_str}. "
            f"reason={reason} (source={bl_info['source']})"
        )
        return {
            "status": "blacklist",
            "item_name": item_name,
            "reason": reason,
        }

    # --- Скачиваем HTML ---
    try:
        html = fetch_html_with_proxies(url, item_name)
    except Exception as e:
        msg = f"Ошибка при скачивании HTML: {e}"
        print(f"[ERROR] {msg}")
        return {
            "status": "error",
            "item_name": item_name,
            "message": msg,
        }

    # --- Парсим продажи ---
    sales: List[Sale] = []
    parse_success = False
    last_error = None

    for attempt in range(1, config.MAX_HTML_RETRIES + 1):
        try:
            sales = parse_sales_from_html(item_name, html)
            parse_success = True
            break
        except Exception as e:
            last_error = e
            print(f"[ERROR] Ошибка парсинга line1 (попытка {attempt}): {e}")
            try:
                html = fetch_html_with_proxies(url, item_name)
            except Exception as e2:
                print(f"[ERROR] Повторная ошибка скачивания HTML: {e2}")
                break

    if not parse_success or not sales:
        failed_path = os.path.join(
            config.HTML_FAILED_DIR, f"{safe_filename(item_name)}_failed.html"
        )
        try:
            with open(failed_path, "w", encoding="utf-8") as f:
                f.write(html)
        except Exception:
            pass

        msg = f"Не удалось распарсить продажи после нескольких попыток. last_error={last_error}"
        print(f"[ERROR] {msg}")
        return {
            "status": "error",
            "item_name": item_name,
            "message": msg,
        }

    metrics = compute_basic_metrics(sales)

    total_amount_30d = sum(s.amount for s in sales)
    if total_amount_30d < config.MIN_TOTAL_AMOUNT_30D:
        reason = f"too_low_volume_30d ({total_amount_30d} < {config.MIN_TOTAL_AMOUNT_30D})"
        return blacklist_with_html(
            item_name, reason, html, base_price=metrics.get("base_price")
        )

    # Ранний фильтр по гэпу должен срабатывать сразу после проверки объёма,
    # чтобы предмет моментально отправлялся в блэклист без дальнейшего анализа.
    max_gap_hours, long_gaps_count = gap_stats_between_points(
        sales,
        config.GAP_FILTER_WINDOW_DAYS,
        config.MAX_GAP_BETWEEN_POINTS_HOURS,
    )
    if long_gaps_count > config.MAX_ALLOWED_LONG_GAPS:
        reason = (
            "gap_between_points_too_big "
            f"({long_gaps_count} > {config.MAX_ALLOWED_LONG_GAPS} gaps "
            f">= {config.MAX_GAP_BETWEEN_POINTS_HOURS:.1f}h within "
            f"{config.GAP_FILTER_WINDOW_DAYS}d; max_gap={max_gap_hours:.1f}h)"
        )
        return blacklist_with_html(
            item_name, reason, html, base_price=metrics.get("base_price")
        )
    dips = compute_price_dips(sales, metrics)

    print(
        f"[ANALYSIS] {item_name}: base={metrics['base_price']:.4f}, "
        f"mean={metrics['mean_price']:.4f}, std={metrics['std_price']:.4f}, "
        f"cv={metrics['cv_price']:.3f}, \n"
        f"trend_30_pts={metrics['trend_rel_30']*100:.1f}%, "
        f"trend_{config.FORECAST_TREND_WINDOW_DAYS}_pts={metrics['trend_rel_recent']*100:.1f}%, "
        f"trend_30_vol={metrics['trend_rel_30_volume']*100:.1f}%, "
        f"trend_30_down_forecast={metrics['trend_rel_30_down_forecast']*100:.1f}%, "
        f"p20={metrics['p20']:.4f}, p80={metrics['p80']:.4f}, "
        f"old_dips={dips['old_dips_days']:.2f}d, "
        f"recent_dips={dips['recent_dips_days']:.2f}d, "
        f"wave_amp_det={metrics['wave_amp_detrended']:.3f}, "
        f"wave_amp_raw={metrics['wave_amp_raw']:.3f}"
    )

    shape_result = classify_shape_and_rec_price(sales, metrics, dips)

    if shape_result["status"] == "blacklist":
        reason = shape_result["reason"]
        print(f"[RESULT] BLACKLIST. {item_name}: {reason}")
        return blacklist_with_html(
            item_name,
            reason,
            html,
            base_price=metrics.get("base_price"),
        )

    rec_price = float(shape_result["rec_price"])
    rec_price_sales = shape_result.get("rec_price_sales", [])
    support_sales = shape_result.get("rec_price_support_sales", rec_price_sales)
    rec_price_from_dip = shape_result.get("rec_price_from_dip", False)
    latest_dip_dt = shape_result.get("latest_dip_dt")
    periods_override = shape_result.get("rec_price_support_periods")

    adjusted_rec_price = enforce_rec_price_sales_support(
        rec_price,
        support_sales,
        dip_based=rec_price_from_dip,
        latest_dip_dt=latest_dip_dt,
        periods_override=periods_override,
    )
    if adjusted_rec_price != rec_price:
        print(
            f"[ADJUST] rec_price lowered from {rec_price:.4f} to {adjusted_rec_price:.4f} "
            "to satisfy sales support."
        )
        rec_price = adjusted_rec_price

    trend_factor = shape_result.get("post_support_trend_factor", 1.0)
    if trend_factor != 1.0:
        rec_price *= trend_factor
        print(
            f"[ADJUST] rec_price trend forecast factor applied: x{trend_factor:.3f}; "
            f"new rec_price={rec_price:.4f}"
        )
    tier = int(shape_result["tier"])
    graph_type = shape_result["graph_type"]
    reason = shape_result["reason"]

    if rec_price < config.MIN_REC_PRICE_USD1:
        reason = (
            "rec_price_below_minimum "
            f"({rec_price:.4f} < {config.MIN_REC_PRICE_USD1:.4f})"
        )
        return blacklist_with_html(
            item_name,
            reason,
            html,
            days=config.MIN_REC_PRICE_BLACKLIST_DAYS1,
            rec_price_blacklist=True,
        )

    if rec_price < config.MIN_REC_PRICE_USD2:
        reason = (
            "rec_price_below_secondary_minimum "
            f"({rec_price:.4f} < {config.MIN_REC_PRICE_USD2:.4f})"
        )
        return blacklist_with_html(
            item_name,
            reason,
            html,
            days=config.MIN_REC_PRICE_BLACKLIST_DAYS2,
            rec_price_blacklist=True,
        )

    avg_sales = compute_avg_sales_last_two_weeks(sales)

    print(
        f"[RESULT] OK. {item_name}: type={graph_type}, tier={tier}, "
        f"rec_price={rec_price:.4f} USD, avg_sales={avg_sales:.2f}"
    )
    print(f"[REASON] {reason}")

    save_item_result(
        item_name=item_name,
        game_code=game_code,
        rec_price=rec_price,
        avg_sales=avg_sales,
        tier=tier,
        graph_type=graph_type,
        sales_points=len(sales),
    )

    html_path = os.path.join(
        config.HTML_APPROVE_DIR,
        f"{safe_filename(item_name)}.html",
    )
    try:
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html)
    except Exception:
        pass

    return {
        "status": "ok",
        "item_name": item_name,
        "rec_price": rec_price,
        "avg_sales": avg_sales,
        "tier": tier,
        "graph_type": graph_type,
        "message": reason,
    }
