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
            sales_points   INTEGER
        )
        """
    )

    # Таблица блэклиста
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS blacklist (
            item_name  TEXT PRIMARY KEY,
            added_at   TEXT,
            expires_at TEXT,
            reason     TEXT
        )
        """
    )

    # Таблица прокси
    cur.execute(
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

    conn.commit()
    conn.close()


def upsert_proxy(address: str) -> None:
    conn = get_conn()
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


def add_to_blacklist(item_name: str, reason: str) -> None:
    now = datetime.utcnow()
    expires = now + timedelta(days=config.BLACKLIST_DAYS)
    conn = get_conn()
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


def get_blacklist_entry(item_name: str) -> Optional[sqlite3.Row]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM blacklist WHERE item_name = ?",
        (item_name,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def remove_from_blacklist(item_name: str) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM blacklist WHERE item_name = ?", (item_name,))
    conn.commit()
    conn.close()


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
      - trend_rel_30: относительный тренд за 30 дней (доля, напр. -0.12 = -12%)
      - slope_per_day: наклон линейной регрессии (в валюте/день)
    """
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
            "trend_rel_30": 0.0,
            "slope_per_day": 0.0,
        }

    sales_sorted = sorted(sales, key=lambda s: s.dt)
    prices = [s.price for s in sales_sorted]
    weights = [s.amount for s in sales_sorted]

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
    t_vals = [(s.dt - first_dt).total_seconds() / 86400.0 for s in sales_sorted]

    sum_w = sum(weights)
    if sum_w <= 0:
        slope = 0.0
        trend_rel_30 = 0.0
    else:
        sum_wt = sum(w * t for t, w in zip(t_vals, weights))
        sum_wp = sum(w * p for p, w in zip(prices, weights))
        sum_wt2 = sum(w * (t ** 2) for t, w in zip(t_vals, weights))
        sum_wtp = sum(w * t * p for t, p, w in zip(t_vals, prices, weights))

        denom = (sum_w * sum_wt2 - sum_wt ** 2)
        if abs(denom) < 1e-9:
            slope = 0.0
        else:
            slope = (sum_w * sum_wtp - sum_wt * sum_wp) / denom

        if base_price > 0:
            trend_rel_30 = (slope * 30.0) / base_price
        else:
            trend_rel_30 = 0.0

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
        "trend_rel_30": trend_rel_30,
        "slope_per_day": slope,
    }


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
    if base_price <= 0:
        return {"old_dips_days": 0.0, "recent_dips_days": 0.0}

    sales_sorted = sorted(sales, key=lambda s: s.dt)
    last_dt = sales_sorted[-1].dt

    volumes = [s.amount for s in sales_sorted]
    medium_volume = sum(volumes) / len(volumes) if volumes else 0.0

    old_dips_days = 0.0
    recent_dips_days = 0.0

    n = len(sales_sorted)
    i = 0
    while i < n:
        s = sales_sorted[i]
        age_days = (last_dt - s.dt).total_seconds() / 86400.0

        if age_days <= config.DIP_RECENT_WINDOW_DAYS:
            low_corridor_rel = config.LOW_CORRIDOR_REL_RECENT
            target_bucket = "recent"

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

        price_threshold = base_price * trend_factor * (1.0 - low_corridor_rel)

        if s.price >= price_threshold:
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
                if s_j.price < price_threshold:
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
    if base_price <= 0:
        return segments

    sales_sorted = sorted(sales, key=lambda s: s.dt)
    last_dt = sales_sorted[-1].dt

    volumes = [s.amount for s in sales_sorted]
    medium_volume = sum(volumes) / len(volumes) if volumes else 0.0

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

        price_threshold = base_price * trend_factor * (1.0 - low_corridor_rel)

        if s.price >= price_threshold:
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
                if s_j.price < price_threshold:
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

def compute_rec_price_default(sales: List[Sale], q: float) -> Tuple[float, List[Sale]]:
    """
    Базовая рек. цена:
      - последние RECENT_DAYS_FOR_REC_PRICE дней,
      - взвешенный квантиль q (зависит от стабильности предмета).
    """
    if not sales:
        return 0.0, []

    sales_sorted = sorted(sales, key=lambda s: s.dt)
    last_dt = sales_sorted[-1].dt
    cutoff = last_dt - timedelta(days=config.RECENT_DAYS_FOR_REC_PRICE)

    recent = [s for s in sales_sorted if s.dt >= cutoff]
    if len(recent) < 10:
        recent = sales_sorted

    prices = [s.price for s in recent]
    weights = [s.amount for s in recent]

    return weighted_quantile(prices, weights, q), recent


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
    trend_rel_30 = metrics["trend_rel_30"]  # отрицательный

    factor = 1.0 + trend_rel_30 * (config.FORECAST_HORIZON_DAYS / 30.0)
    if factor < 0.0:
        factor = 0.0

    return base_rec * factor, base_sales


def compute_rec_price_wave(
    sales: List[Sale], metrics: Dict[str, float]
) -> Tuple[float, List[Sale]]:
    """
    Для волновых графиков:
      - берём только точки из валидных провалов (dips),
      - считаем по ним взвешенный квантиль REC_WAVE_Q.
      - если провалов нет (по нашим жёстким критериям) – fallback к default.
    """
    segments = find_valid_dip_segments(sales, metrics)
    if not segments:
        return compute_rec_price_default(
            sales, config.REC_PRICE_LOWER_Q_VOLATILE
        )

    dip_sales: List[Sale] = []
    for seg in segments:
        dip_sales.extend(seg)

    prices = [s.price for s in dip_sales]
    weights = [s.amount for s in dip_sales]

    return weighted_quantile(prices, weights, config.REC_WAVE_Q), dip_sales


def enforce_rec_price_sales_support(
    rec_price: float, rec_sales: List[Sale]
) -> float:
    """
    Проверяет, что в последней половине диапазона, использованного для расчёта rec_price,
    каждые REC_PRICE_SUPPORT_STEP_HOURS внутри окна REC_PRICE_SUPPORT_WINDOW_HOURS
    есть достаточный объём продаж на цене >= rec_price. При нехватке продаж rec_price
    опускается до максимального уровня, при котором условие выполняется во всех окнах.
    """
    if rec_price <= 0 or not rec_sales:
        return rec_price

    sales_sorted = sorted(rec_sales, key=lambda s: s.dt)
    window_start = sales_sorted[0].dt
    window_end = sales_sorted[-1].dt

    if window_start >= window_end:
        return rec_price

    half_start = window_start + (window_end - window_start) / 2
    relevant_sales = [s for s in sales_sorted if s.dt >= half_start]

    if not relevant_sales:
        return rec_price

    step = timedelta(hours=config.REC_PRICE_SUPPORT_STEP_HOURS)
    window = timedelta(hours=config.REC_PRICE_SUPPORT_WINDOW_HOURS)
    min_share = config.REC_PRICE_SUPPORT_MIN_SHARE

    adjusted_price = rec_price
    t = half_start
    while t <= window_end:
        window_end_ts = min(t + window, window_end)
        window_sales = [s for s in relevant_sales if t <= s.dt <= window_end_ts]
        total_vol = sum(s.amount for s in window_sales)

        if total_vol <= 0:
            adjusted_price = min(adjusted_price, 0.0)
        else:
            required_vol = total_vol * min_share
            acc = 0
            candidate_price = 0.0
            for sale in sorted(window_sales, key=lambda s: s.price, reverse=True):
                acc += sale.amount
                candidate_price = sale.price
                if acc >= required_vol:
                    break
            adjusted_price = min(adjusted_price, candidate_price)

        t += step

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
      - wave,        tier=4
      - volatile,    tier=4
    Возможен также status="blacklist" (слишком много дипов или сильный спад).
    """
    base = metrics["base_price"]
    trend = metrics["trend_rel_30"]
    cv = metrics["cv_price"]
    p20 = metrics["p20"]
    p80 = metrics["p80"]

    if base > 0:
        wave_amp = (p80 - p20) / base
    else:
        wave_amp = 0.0

    old_dips = dips.get("old_dips_days", 0.0)
    recent_dips = dips.get("recent_dips_days", 0.0)
    total_dip_days = old_dips + recent_dips

    # 1) Жёсткие отсечки по провалам
    if old_dips > config.DIP_MAX_OLD_DAYS:
        rec_price, rec_sales = compute_rec_price_wave(sales, metrics)
        return {
            "status": "ok",
            "graph_type": "volatile",
            "tier": 4,
            "rec_price": rec_price,
            "rec_price_sales": rec_sales,
            "reason": f"old_dips_excess ({old_dips:.2f} days); dip-based rec_price",
        }

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

    # shape_ok = спокойный график по разбросу
    shape_ok = (cv <= config.STABLE_MAX_CV and wave_amp <= config.STABLE_MAX_WAVE_AMP)

    # 3) stable_flat (tier 1): тренд -5..+5%, форма графика спокойная
    if abs(trend) <= config.TREND_REL_FLAT_MAX and shape_ok:
        rec_price, rec_price_sales = compute_rec_price_default(
            sales, config.REC_PRICE_LOWER_Q_STABLE
        )
        return {
            "status": "ok",
            "graph_type": "stable_flat",
            "tier": 1,
            "rec_price": rec_price,
            "rec_price_sales": rec_price_sales,
            "reason": f"stable_flat; trend={trend*100:.1f}%, cv={cv:.3f}, wave_amp={wave_amp:.3f}",
        }

    # 4) stable_up (tier 2): тренд вверх, не слишком резкий, форма спокойная
    if trend > config.TREND_REL_FLAT_MAX and trend <= config.MAX_UP_TREND_REL and shape_ok:
        rec_price, rec_price_sales = compute_rec_price_default(
            sales, config.REC_PRICE_LOWER_Q_STABLE
        )
        return {
            "status": "ok",
            "graph_type": "stable_up",
            "tier": 2,
            "rec_price": rec_price,
            "rec_price_sales": rec_price_sales,
            "reason": f"stable_up; trend={trend*100:.1f}%, cv={cv:.3f}, wave_amp={wave_amp:.3f}",
        }

    # 5) stable_down (tier 3): тренд вниз, умеренный, форма спокойная
    if trend < -config.TREND_REL_FLAT_MAX and trend >= config.MAX_DOWN_TREND_REL and shape_ok:
        rec_price, rec_price_sales = compute_rec_price_downtrend(sales, metrics)
        return {
            "status": "ok",
            "graph_type": "stable_down",
            "tier": 3,
            "rec_price": rec_price,
            "rec_price_sales": rec_price_sales,
            "reason": f"stable_down; trend={trend*100:.1f}%, cv={cv:.3f}, wave_amp={wave_amp:.3f}",
        }

    # 6) wave (tier 4): большая амплитуда + есть существенные провалы
    if (
        allow_wave
        and wave_amp >= config.WAVE_MIN_AMP
        and total_dip_days >= config.WAVE_MIN_DIP_DAYS
    ):
        rec_price, rec_price_sales = compute_rec_price_wave(sales, metrics)
        return {
            "status": "ok",
            "graph_type": "wave",
            "tier": 4,
            "rec_price": rec_price,
            "rec_price_sales": rec_price_sales,
            "reason": f"wave; trend={trend*100:.1f}%, cv={cv:.3f}, "
                      f"wave_amp={wave_amp:.3f}, dip_days={total_dip_days:.2f}",
        }

    # 7) всё остальное – нестабильные / сложные графики (tier 4)
    rec_price, rec_price_sales = compute_rec_price_default(
        sales, config.REC_PRICE_LOWER_Q_VOLATILE
    )
    return {
        "status": "ok",
        "graph_type": "volatile",
        "tier": 4,
        "rec_price": rec_price,
        "rec_price_sales": rec_price_sales,
        "reason": f"volatile; trend={trend*100:.1f}%, cv={cv:.3f}, wave_amp={wave_amp:.3f}",
    }


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
    with open(fn, "w", encoding="utf-8") as f:
        f.write("price\tamount\ttime\tdate\n")
        for s in sales:
            f.write(
                f"{s.price:.6f}\t{s.amount}\t"
                f"{s.dt.strftime('%H:%M')}\t{s.dt.strftime('%Y-%m-%d')}\n"
            )


# ---------- Работа с прокси и скачивание HTML ----------
# (эта часть не менялась, оставляю как в предыдущей версии)

def get_all_proxies() -> List[sqlite3.Row]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM proxies WHERE disabled = 0 ORDER BY id ASC")
    rows = cur.fetchall()
    conn.close()
    return rows


def update_proxy_row(
    proxy_id: int,
    *,
    last_used_ts: Optional[float] = None,
    rest_until_ts: Optional[float] = None,
    fail_stage: Optional[int] = None,
    fallback_fail_count: Optional[int] = None,
    disabled: Optional[int] = None,
) -> None:
    conn = get_conn()
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

    if config.PROXY_SELECT == 0:
        attempt = 0
        while attempt < config.MAX_HTML_RETRIES:
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
            "rest_until_ts": 0.0,
            "fail_stage": 0,
            "fallback_fail_count": 0,
            "disabled": 0,
        }

    proxy_cycle = list(proxies_rows)
    if direct_entry is not None:
        proxy_cycle.append(direct_entry)

    cycle_len = len(proxy_cycle)
    if cycle_len == 0:
        return fetch_html_direct(url)

    start_index = 0
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
                rest_until_ts = 0.0
                last_used_ts = 0.0
                fail_stage = 0
                fallback_fail_count = 0
            else:
                use_direct = False
                proxy_id = row["id"]
                rest_until_ts = row["rest_until_ts"] or 0.0
                last_used_ts = row["last_used_ts"] or 0.0
                fail_stage = row["fail_stage"] or 0
                fallback_fail_count = row["fallback_fail_count"] or 0

            now_ts = time.time()

            if not use_direct and rest_until_ts > now_ts:
                remain = int(rest_until_ts - now_ts)
                print(f"[PROXY] Прокси {row['address']} ещё отдыхает {remain} сек.")
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

                    if not use_direct and proxy_id is not None:
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

                return html

            except requests.RequestException as e:
                print(
                    f"[DOWNLOAD] Сетевая ошибка через "
                    f"{row['address'] if not use_direct else 'DIRECT'}: {e}"
                )
                if not use_direct and proxy_id is not None:
                    try:
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

                        return html
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
    bl = get_blacklist_entry(item_name)
    if bl is not None:
        expires_at_str = bl["expires_at"]
        try:
            expires_at = datetime.fromisoformat(expires_at_str)
        except Exception:
            expires_at = None

        if expires_at is not None and expires_at > datetime.utcnow():
            reason = bl["reason"]
            print(f"[BLACKLIST] {item_name} в блэклисте до {expires_at_str}. reason={reason}")
            return {
                "status": "blacklist",
                "item_name": item_name,
                "reason": reason,
            }
        else:
            print(f"[BLACKLIST] {item_name}: срок блэклиста истёк, удаляем.")
            remove_from_blacklist(item_name)

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

    total_amount_30d = sum(s.amount for s in sales)
    if total_amount_30d < config.MIN_TOTAL_AMOUNT_30D:
        reason = f"too_low_volume_30d ({total_amount_30d} < {config.MIN_TOTAL_AMOUNT_30D})"
        print(f"[ANALYSIS] {item_name}: {reason}")
        add_to_blacklist(item_name, reason)
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

    metrics = compute_basic_metrics(sales)
    dips = compute_price_dips(sales, metrics)

    print(
        f"[ANALYSIS] {item_name}: base={metrics['base_price']:.4f}, "
        f"mean={metrics['mean_price']:.4f}, std={metrics['std_price']:.4f}, "
        f"cv={metrics['cv_price']:.3f}, trend_30={metrics['trend_rel_30']*100:.1f}%, "
        f"p20={metrics['p20']:.4f}, p80={metrics['p80']:.4f}, "
        f"old_dips={dips['old_dips_days']:.2f}d, "
        f"recent_dips={dips['recent_dips_days']:.2f}d"
    )

    shape_result = classify_shape_and_rec_price(sales, metrics, dips)

    if shape_result["status"] == "blacklist":
        reason = shape_result["reason"]
        print(f"[RESULT] BLACKLIST. {item_name}: {reason}")
        add_to_blacklist(item_name, reason)
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

    rec_price = float(shape_result["rec_price"])
    rec_price_sales = shape_result.get("rec_price_sales", [])
    adjusted_rec_price = enforce_rec_price_sales_support(rec_price, rec_price_sales)
    if adjusted_rec_price != rec_price:
        print(
            f"[ADJUST] rec_price lowered from {rec_price:.4f} to {adjusted_rec_price:.4f} "
            "to satisfy sales support."
        )
        rec_price = adjusted_rec_price
    tier = int(shape_result["tier"])
    graph_type = shape_result["graph_type"]
    reason = shape_result["reason"]

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
