# config.py - configuration for Steam price analyser

# ------------ General paths ------------
DB_PATH = "steam_analyser.db"

# Отдельная база для прокси
PROXY_DB_PATH = "steam_analyser_proxies.db"

# Отдельная база для блэклиста
BLACKLIST_DB_PATH = "steam_analyser_blacklist.db"

PROXIES_FILE = "proxies.txt"

HTML_APPROVE_DIR = "HTML_steam_approve"
HTML_BLACKLIST_DIR = "HTML_steam_blacklist"
HTML_FAILED_DIR = "HTML_steam_failed"
SELL_PARSING_DIR = "sell_parsing"
HTML_TEMP_DIR = "HTML_temp"

# ------------ Liss bot settings ------------

# Список ключевых слов, при наличии которых предмет отбрасывается
LISS_BLACKLIST_KEYWORDS = []

# Основной и дополнительный фильтры по цене лота, USD
LISS_MIN_PRICE = 0.0
LISS_EXTRA_MIN_PRICE = 0.0
LISS_MAX_PRICE = 1000.0

# Минимально допустимая прибыль лота (price / ((rec_price * 0.8697) - 1))
LISS_MIN_PROFIT = 0.0

# Максимальный срок холда предмета (0–8 дней)
LISS_MAX_HOLD_DAYS = 0

# Ограничения по количеству и сумме покупок
LISS_QUANTITY_PERCENT = 10.0  # доля от средних продаж в неделю на Steam
LISS_PERIOD_DAYS = 3  # период учёта для лимитов
LISS_SUM_LIMIT = 50.0  # максимальная сумма покупок одного предмета за период

# ------------ Caching / DB freshness ------------

# Сколько часов результат по предмету считается актуальным
ACTUAL_HOURS = 0.0

# Сколько дней предмет держится в блэклисте
BLACKLIST_DAYS = 7.0

# ------------ HTTP / proxies ------------

# 0 = без прокси (только прямой IP)
# 1 = только прокси
# 2 = прокси + прямой IP как отдельный "прокси" в очереди
PROXY_SELECT = 0

# 1 = без задержки, 0 = учитывать задержку между запросами через один и тот же прокси
DELAY_OFF = 0

# Минимальная задержка (в секундах) между двумя запросами через один и тот же прокси
DELAY_HTML = 30.0

# Отдых прокси после первого 429 Too Many Requests
REST_PROXY1 = 300.0  # 5 минут

# Отдых прокси после второго 429 сразу после окончания первого отдыха
REST_PROXY2 = 1800.0  # 30 минут

# Задержка между повторными попытками скачивания страницы при сетевых ошибках (в секундах)
DELAY_DOWNLOAD_ERROR = 15.0

# Max number of times when proxy is marked as "bad" because direct IP
# succeeded while proxy failed (ПОДРЯД)
MAX_FALLBACK_FAILS = 3

# Максимальное количество полных попыток скачивания HTML (с разными прокси)
MAX_HTML_RETRIES = 3

# Таймаут HTTP-запроса к Steam, секунд
HTTP_TIMEOUT = 20.0

# ------------ Аналитика продаж ------------

# Минимальное количество проданных штук за 30 дней, чтобы вообще анализировать предмет
MIN_TOTAL_AMOUNT_30D = 150

# Фильтр выбросов цен для расчёта базовых метрик и трендов.
# Метод "quantile" использует коридор между PRICE_OUTLIER_QUANTILES (винсоризация),
# метод "iqr" строит коридор [Q1 - k*IQR, Q3 + k*IQR] с множителем PRICE_OUTLIER_IQR_MULT.
PRICE_OUTLIER_METHOD = "quantile"  # "quantile" или "iqr"
PRICE_OUTLIER_QUANTILES = (0.05, 0.95)
PRICE_OUTLIER_IQR_MULT = 1.5

# ------------ Разрывы между точками графика ------------

# Максимально допустимый гэп между соседними точками (в часах)
MAX_GAP_BETWEEN_POINTS_HOURS = 13.0

# Сколько гэпов длительностью >= MAX_GAP_BETWEEN_POINTS_HOURS допускается
# за последние GAP_FILTER_WINDOW_DAYS (0 = не допускаются вовсе)
MAX_ALLOWED_LONG_GAPS = 3

# Сколько последних дней учитывать при проверке гэпа между точками
GAP_FILTER_WINDOW_DAYS = 21

# ------------ Просадки по цене (dips) ------------

# Глубина провала вниз от базовой цены для старой части графика
LOW_CORRIDOR_REL_OLD = 0.15   # 15% ниже base_price

# Глубина провала вниз от базовой цены для свежей части графика (последние DIP_RECENT_WINDOW_DAYS)
LOW_CORRIDOR_REL_RECENT = 0.15

# Где проходит граница между "старым" и "свежим" участком (в днях от последней продажи)
DIP_RECENT_WINDOW_DAYS = 7.0

# Минимальное количество подряд идущих точек ниже порога, чтобы считать провал
DIP_MIN_POINTS = 2

# Требуемый объём провала относительно обычного среднего объёма за точку:
# dip_volume >= medium_volume_per_point * N_points * DIP_VOLUME_FACTOR
DIP_VOLUME_FACTOR = 0.5

# Максимальная суммарная длительность валидных провалов в старой части (в днях)
DIP_MAX_OLD_DAYS = 5.0

# Максимальная суммарная длительность валидных провалов в свежей части (в днях)
DIP_MAX_RECENT_DAYS = 0.5  # 0.5 суток ≈ 12 часов

# ------------ Тренды и форма графика ------------

# "Ровный" тренд: ±5% за 30 дней
TREND_REL_FLAT_MAX = 0.08

# Максимальный коэффициент вариации для стабильного графика
STABLE_MAX_CV = 0.18

# Максимальная относительная амплитуда между p20 и p80 (от base_price) для stable
STABLE_MAX_WAVE_AMP = 0.2

# Uptrend / downtrend thresholds:
# максимально допустимый рост для "stable_up" (например +50% за 30 дней)
MAX_UP_TREND_REL = 0.35

# сильный нисходящий тренд (меньше этого) → сразу блэклист
MAX_DOWN_TREND_REL = -0.35  # -40% за месяц

# ------------ Рекомендованная цена (общая) ------------

# Сколько последних дней учитывать при расчёте рек. цены
RECENT_DAYS_FOR_REC_PRICE = 14
# Сколько последних дней учитывать для расчёта рек. цены на восходящих трендах
RECENT_DAYS_FOR_UPTREND_REC_PRICE = 14

# Раздельные квантили для стабильных и волатильных предметов
# 0.40 → 40-й перцентиль (60% объёма продаж выше этой цены)
REC_PRICE_LOWER_Q_STABLE = 0.4
REC_PRICE_LOWER_Q_VOLATILE = 0.35

# Проверка достаточной поддержки продаж около рек. цены
# На нескольких временных промежутках проверяется, что вокруг rec_price есть
# достаточный объём продаж. Для каждого периода задаются:
#   - HOURS: глубина в часах от последней продажи, в пределах которой строятся окна;
#   - STEP_WINDOW_HOURS: единая величина для шага между окнами и их ширины;
#   - MIN_SHARE: требуемая доля объёма продаж на цене >= rec_price внутри окна;
#   - MAX_ALLOWED_VIOLATIONS: сколько окон в пределах периода могут нарушать условие,
#     не требуя снижения rec_price.
REC_PRICE_SUPPORT_PERIODS = [
    {
        "HOURS": 24.0,
        "STEP_WINDOW_HOURS": 12.0,
        "MIN_SHARE": 0.3,
        "MAX_ALLOWED_VIOLATIONS": 0,
        "MIN_WINDOW_VOLUME": 2,
    },
    {
        "HOURS": 168.0,
        "STEP_WINDOW_HOURS": 18.0,
        "MIN_SHARE": 0.3,
        "MAX_ALLOWED_VIOLATIONS": 2,
        "MIN_WINDOW_VOLUME": 5,
    },
]
# Минимальный суммарный объём продаж в окне, чтобы проверка считалась значимой
# и единичные сделки не занижали rec_price (используется как fallback).
REC_PRICE_SUPPORT_MIN_WINDOW_VOLUME = 5

# ------------ Волновые графики (wave) ------------

# Минимальная амплитуда для wave: (p80 - p20) / base_price
WAVE_MIN_AMP = 0.35  # 40%

# Минимальная суммарная длительность провалов (old+recent), чтобы вообще говорить о "волнах"
WAVE_MIN_DIP_DAYS = 0.3  # ~ 7 часов

# Квантиль для rec_price по дипам (volume-weighted)
REC_WAVE_Q = 0.35

# Медианная цена для recent dips считается по последним N дням
RECENT_DIP_MEDIAN_DAYS = 10.0

# ------------ Нисходящий тренд: прогноз ------------

# Горизонт прогноза в днях (между 7 и 14, ты предлагал 10)
FORECAST_HORIZON_DAYS = 10.0
# Окно точек (в днях) для расчёта тренда при прогнозировании нисходящего графика
FORECAST_TREND_WINDOW_DAYS = 21.0

# ------------ Boost / Crash detection ------------

# Сколько последних дней считать "boost/crash-зоной"
BOOST_RECENT_DAYS = 7.0

# Условие буста: base_recent >= base_old * BOOST_MIN_RATIO
BOOST_MIN_RATIO = 1.30  # рост на 30% и более

# Условие краша: base_recent <= base_old * CRASH_MIN_RATIO
CRASH_MIN_RATIO = 0.75  # падение на 25% и более

# Минимальный объём в старой и новой части для надёжного детекта
BOOST_MIN_OLD_VOLUME = 50
BOOST_MIN_RECENT_VOLUME = 50
