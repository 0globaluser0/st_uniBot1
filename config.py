# config.py - configuration for Steam price analyser

# ------------ General paths ------------
DB_PATH = "steam_analyser.db"

PROXIES_FILE = "proxies.txt"

HTML_APPROVE_DIR = "HTML_steam_approve"
HTML_BLACKLIST_DIR = "HTML_steam_blacklist"
HTML_FAILED_DIR = "HTML_steam_failed"
SELL_PARSING_DIR = "sell_parsing"
HTML_TEMP_DIR = "HTML_temp"

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
MIN_TOTAL_AMOUNT_30D = 50

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
STABLE_MAX_CV = 0.2

# Максимальная относительная амплитуда между p20 и p80 (от base_price) для stable
STABLE_MAX_WAVE_AMP = 0.23

# Uptrend / downtrend thresholds:
# максимально допустимый рост для "stable_up" (например +50% за 30 дней)
MAX_UP_TREND_REL = 0.50

# сильный нисходящий тренд (меньше этого) → сразу блэклист
MAX_DOWN_TREND_REL = -0.3  # -40% за месяц

# ------------ Рекомендованная цена (общая) ------------

# Сколько последних дней учитывать при расчёте рек. цены
RECENT_DAYS_FOR_REC_PRICE = 14

# Раздельные квантили для стабильных и волатильных предметов
# 0.40 → 40-й перцентиль (60% объёма продаж выше этой цены)
REC_PRICE_LOWER_Q_STABLE = 0.4
REC_PRICE_LOWER_Q_VOLATILE = 0.35

# Проверка достаточной поддержки продаж около рек. цены
# На последней половине диапазона, использованного для расчёта рек. цены,
# каждые REC_PRICE_SUPPORT_STEP_HOURS часов внутри окна REC_PRICE_SUPPORT_WINDOW_HOURS
# должны быть продажи на цене >= rec_price в объёме не меньше
# REC_PRICE_SUPPORT_MIN_SHARE доли от всего объёма окна. Иначе rec_price опускается
# до максимального уровня, при котором условие выполняется во всех окнах.
REC_PRICE_SUPPORT_STEP_HOURS = 12.0
REC_PRICE_SUPPORT_WINDOW_HOURS = 12.0
REC_PRICE_SUPPORT_MIN_SHARE = 0.3

# ------------ Волновые графики (wave) ------------

# Минимальная амплитуда для wave: (p80 - p20) / base_price
WAVE_MIN_AMP = 0.35  # 40%

# Минимальная суммарная длительность провалов (old+recent), чтобы вообще говорить о "волнах"
WAVE_MIN_DIP_DAYS = 0.3  # ~ 7 часов

# Квантиль для rec_price по дипам (volume-weighted)
REC_WAVE_Q = 0.35

# ------------ Нисходящий тренд: прогноз ------------

# Горизонт прогноза в днях (между 7 и 14, ты предлагал 10)
FORECAST_HORIZON_DAYS = 11.0

# ------------ Boost / Crash detection ------------

# Сколько последних дней считать "boost/crash-зоной"
BOOST_RECENT_DAYS = 7.0

# Условие буста: base_recent >= base_old * BOOST_MIN_RATIO
BOOST_MIN_RATIO = 1.30  # рост на 30% и более

# Условие краша: base_recent <= base_old * CRASH_MIN_RATIO
CRASH_MIN_RATIO = 0.65  # падение на 25% и более

# Минимальный объём в старой и новой части для надёжного детекта
BOOST_MIN_OLD_VOLUME = 50
BOOST_MIN_RECENT_VOLUME = 50
