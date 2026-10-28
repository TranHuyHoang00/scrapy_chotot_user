"""Settings theo cấu trúc spider Hydra.

Giá trị máy chủ, key, mật khẩu KHÔNG ghi trong code: đặt qua biến môi trường
(Crawlab: phần Environment của spider) hoặc file .env khi chạy local.
Xem .env.example để biết danh sách biến.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

from chotot.utils.redis_client import redis_url

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


def env(name, default=None):
    value = os.getenv(name)
    return default if value in (None, "") else value


def env_bool(name, default=False):
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


def env_int(name, default):
    return int(env(name, default))


def env_float(name, default):
    return float(env(name, default))


def env_list(name, default):
    return [s.strip() for s in env(name, default).split(",") if s.strip()]


# ===== Scrapy =====
BOT_NAME = "chotot"
SPIDER_MODULES = ["chotot.spiders"]
NEWSPIDER_MODULE = "chotot.spiders"

ROBOTSTXT_OBEY = False
COOKIES_ENABLED = False
TELNETCONSOLE_ENABLED = False

CONCURRENT_REQUESTS = env_int("CONCURRENT_REQUESTS", 8)
CONCURRENT_REQUESTS_PER_DOMAIN = env_int("CONCURRENT_REQUESTS_PER_DOMAIN", 4)
DOWNLOAD_DELAY = env_float("DOWNLOAD_DELAY", 1.5)
DOWNLOAD_TIMEOUT = 30

AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 10
AUTOTHROTTLE_TARGET_CONCURRENCY = 2.0

RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [408, 429, 500, 502, 503, 504, 522, 524]

DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
}

DOWNLOADER_MIDDLEWARES = {
    "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
    "chotot.middlewares.RandomUserAgentMiddleware": 400,
    "chotot.middlewares.ProxyMiddleware": 410,
}

ITEM_PIPELINES = {
    "chotot.pipelines.normalize.NormalizePipeline": 100,
    "chotot.pipelines.validation.ValidationPipeline": 200,
    "chotot.pipelines.redis_dedup.RedisDedupPipeline": 300,
    "chotot.pipelines.kafka_pipeline.HydraKafkaPipeline": 400,
}

EXTENSIONS = {
    "chotot.extensions.CrawlMonitor": 500,
}

REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"

# Dừng sau N item hợp lệ (0 = không giới hạn)
CLOSESPIDER_ITEMCOUNT = env_int("TARGET_ITEM_COUNT", 0)

# ===== Log =====
LOG_ENABLED = True
LOG_LEVEL = env("LOG_LEVEL", "INFO")
LOG_FORMAT = f"%(asctime)s [{BOT_NAME}] %(levelname)s: %(message)s"
LOG_DATEFORMAT = "%Y-%m-%d %H:%M:%S"
LOG_DIR = str(BASE_DIR / "logs")
MONITOR_ENABLED = env_bool("MONITOR_ENABLED", True)
MONITOR_PROGRESS_EVERY = env_int("MONITOR_PROGRESS_EVERY", 50)

# ===== Crawl chotot =====
CHOTOT_START_URL = env("CHOTOT_START_URL", "https://www.chotot.com/mua-ban")  # trang tổng hợp toàn bộ tin
CHOTOT_PAGE_PARAM = "page"
CHOTOT_MAX_PAGES = env_int("CHOTOT_MAX_PAGES", 5)                    # số trang/danh mục nếu profile không khai báo
CHOTOT_MAX_ITEMS_PER_PAGE = env_int("CHOTOT_MAX_ITEMS_PER_PAGE", 0)  # 0 = lấy hết

# Bật/tắt crawl từng site. false: gặp tin của site đó (trên trang danh sách / detail_url) thì bỏ qua, không tải
CRAWL_CHOTOT = env_bool("CRAWL_CHOTOT", True)            # chotot.com
CRAWL_XE = env_bool("CRAWL_XE", True)                    # xe.chotot.com
CRAWL_VIECLAMTOT = env_bool("CRAWL_VIECLAMTOT", True)    # vieclamtot.com
CRAWL_NHATOT = env_bool("CRAWL_NHATOT", True)            # nhatot.com

# Trường bắt buộc theo site; site không khai báo dùng "default"
# Không bắt buộc price: tin cho tặng, thỏa thuận, không ghi giá vẫn giữ (price = null)
VALIDATION_RULES = {
    "default": ["ad_id", "url", "title"],
}
VALIDATION_MIN_TITLE_LENGTH = 3
DEDUP_ALLOW_UPDATES = True
# Dấu vân tay chống trùng hết hạn sau N ngày: quá hạn thì gửi lại 1 lần để đồng bộ, Redis tự dọn. 0 = không hết hạn
DEDUP_TTL_DAYS = env_int("DEDUP_TTL_DAYS", 30)

# ===== Proxy =====
USE_PROXY = env_bool("USE_PROXY", False)
CRAWL_PROXY = env("CRAWL_PROXY")

# ===== Kafka =====
# broker: kết nối thẳng Kafka (Hydra) | http: gửi qua API publish-message (Kafka dev)
KAFKA_MODE = env("KAFKA_MODE", "broker")
KAFKA_HTTP_URL = env("KAFKA_HTTP_URL")
KAFKA_HTTP_API_KEY = env("KAFKA_HTTP_API_KEY")
KAFKA_HTTP_BATCH_SIZE = env_int("KAFKA_HTTP_BATCH_SIZE", 50)
KAFKA_HTTP_TIMEOUT = env_int("KAFKA_HTTP_TIMEOUT", 30)
KAFKA_HTTP_VERIFY_SSL = env_bool("KAFKA_HTTP_VERIFY_SSL", True)

KAFKA_BOOTSTRAP_SERVERS = env_list("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_SECURITY_PROTOCOL = env("KAFKA_SECURITY_PROTOCOL", "SSL")
KAFKA_SSL_CAFILE = env("KAFKA_SSL_CAFILE", "chotot/ssl/ca.pem")
KAFKA_SSL_KEYFILE = env("KAFKA_SSL_KEYFILE", "chotot/ssl/key.pem")
KAFKA_SSL_CERTFILE = env("KAFKA_SSL_CERTFILE", "chotot/ssl/certificate.pem")
KAFKA_SSL_CHECK_HOSTNAME = env_bool("KAFKA_SSL_CHECK_HOSTNAME", False)
KAFKA_API_VERSION = env("KAFKA_API_VERSION", "")  # vd 1.0.0 như spider mẫu; trống = tự dò
KAFKA_GROUP_ID = env("KAFKA_GROUP_ID", "group-chotot")
KAFKA_AUTO_OFFSET_RESET = env("KAFKA_AUTO_OFFSET_RESET", "earliest")
KAFKA_POLL_TIMEOUT_MS = env_int("KAFKA_POLL_TIMEOUT_MS", 50000)
KAFKA_POLL_MAX_RECORDS = env_int("KAFKA_POLL_MAX_RECORDS", 500)

# ===== Kafka local (docker compose --profile kafka up -d) =====
# true: dùng Kafka local thay cho Kafka dev/Hydra. Ghi đè KAFKA_MODE=broker, KAFKA_BOOTSTRAP_SERVERS,
# KAFKA_SECURITY_PROTOCOL=PLAINTEXT (không SSL), KAFKA_API_VERSION tự dò. Tên topic giữ nguyên như dưới.
KAFKA_LOCAL = env_bool("KAFKA_LOCAL", False)
KAFKA_LOCAL_BOOTSTRAP_SERVERS = env_list(
    "KAFKA_LOCAL_BOOTSTRAP_SERVERS", f"localhost:{env('KAFKA_LOCAL_PORT', '9092')}"
)
if KAFKA_LOCAL:
    KAFKA_MODE = "broker"
    KAFKA_BOOTSTRAP_SERVERS = KAFKA_LOCAL_BOOTSTRAP_SERVERS
    KAFKA_SECURITY_PROTOCOL = "PLAINTEXT"
    KAFKA_API_VERSION = ""

KAFKA_TOPIC_INPUT = env("KAFKA_TOPIC_INPUT", "hydra-profile-scheduler-chotot")
# 4 topic output (cấu trúc message: chotot/hydra_topics.py)
KAFKA_TOPIC_POST = env("KAFKA_TOPIC_POST", "hydra-chotot-post_lss-h")                                 # thông tin bài đăng
KAFKA_TOPIC_POST_IMAGES = env("KAFKA_TOPIC_POST_IMAGES", "hydra-images-incoming")               # ảnh bài đăng
KAFKA_TOPIC_PROFILE_BIO = env("KAFKA_TOPIC_PROFILE_BIO", "hydra-profile-bio-chotot_lss-h")            # thông tin người đăng
KAFKA_TOPIC_PROFILE_AVATAR = env("KAFKA_TOPIC_PROFILE_AVATAR", "hydra-images-incoming-avatar")  # ảnh người đăng
KAFKA_TOPIC_LOG = env("KAFKA_TOPIC_LOG", "hydra-spider-log")
KAFKA_LOGGING_ENABLED = env_bool("KAFKA_LOGGING_ENABLED", True)

# ===== Redis (chống trùng item) =====
REDIS_URL = redis_url()

# ===== Elasticsearch / API profile =====
ES_HOST = env("ES_HOST")                              # trống = bỏ qua cập nhật crawled_time
ES_INDEX_CRAWLED_TIME = env("ES_INDEX_CRAWLED_TIME", "hydra-profiles")
END_POINT_API = env("END_POINT_API")                  # trống = bỏ qua cập nhật source_status
API_KEY_GLOBAL = env("API_KEY_GLOBAL")

# ===== API lấy số điện thoại đầy đủ (chotot/utils/phone_api.py) =====
# Đi qua proxy giống crawl web (USE_PROXY / CRAWL_PROXY)
PHONE_API_ENABLED = env_bool("PHONE_API_ENABLED", True)
PHONE_API_URL = env("PHONE_API_URL", "https://gateway.chotot.com/v1/private/ad-listing/phone")
# Bearer token lấy từ API token (GET, header X-Session-Keeper-Key), không đặt trong .env
PHONE_TOKEN_API_URL = env("PHONE_TOKEN_API_URL")
PHONE_TOKEN_API_KEY = env("PHONE_TOKEN_API_KEY")        # giá trị header X-Session-Keeper-Key, không commit
PHONE_TOKEN_RETRY_DELAY = env_float("PHONE_TOKEN_RETRY_DELAY", 60)  # giây chờ trước mỗi lần lấy lại token
PHONE_TOKEN_MAX_RETRIES = env_int("PHONE_TOKEN_MAX_RETRIES", 3)     # hết số lần thì dừng spider
PHONE_API_TIMEOUT = env_int("PHONE_API_TIMEOUT", 15)
PHONE_API_DELAY = env_float("PHONE_API_DELAY", 1.5)     # giây nghỉ giữa 2 lần gọi API
PHONE_API_CONCURRENCY = env_int("PHONE_API_CONCURRENCY", 2)
PHONE_CACHE_TTL_HOURS = env_int("PHONE_CACHE_TTL_HOURS", 168)  # cache số trong Redis, 0 = tắt

# ===== Trang người bán https://www.chotot.com/user/<account_oid> (chotot/utils/seller_profile.py) =====
# Lấy address, joined_at, is_active, phone_verified... cho seller; mỗi người bán tải 1 lần, cache Redis
SELLER_PROFILE_ENABLED = env_bool("SELLER_PROFILE_ENABLED", True)
SELLER_PROFILE_CACHE_TTL_HOURS = env_int("SELLER_PROFILE_CACHE_TTL_HOURS", 72)  # 0 = không cache Redis

# ===== API số liệu người bán (chotot/utils/seller_stats.py) =====
# 6 API public gateway.chotot.com theo account_oid: followers, followings, sold_ads, live_ads, rating, total_rating,
# total_rating_as_seller/buyer, response_rate, response_rate_text, response_time, response_time_text
SELLER_STATS_ENABLED = env_bool("SELLER_STATS_ENABLED", True)
SELLER_STATS_CACHE_TTL_HOURS = env_int("SELLER_STATS_CACHE_TTL_HOURS", 24)  # 0 = không cache Redis
SELLER_STATS_DELAY = env_float("SELLER_STATS_DELAY", 0.5)       # giây nghỉ giữa 2 request API số liệu
SELLER_STATS_CONCURRENCY = env_int("SELLER_STATS_CONCURRENCY", 2)

# Tốc độ riêng cho domain API, tách khỏi tốc độ crawl web
DOWNLOAD_SLOTS = {
    # API số liệu người bán: slot riêng (meta download_slot) dù cùng domain gateway.chotot.com
    "seller-stats": {
        "concurrency": SELLER_STATS_CONCURRENCY,
        "delay": SELLER_STATS_DELAY,
    },
    "gateway.chotot.com": {
        "concurrency": PHONE_API_CONCURRENCY,
        "delay": PHONE_API_DELAY,
        # không khai báo randomize_delay/jitter: dùng mặc định ±50% (RANDOMIZE_DOWNLOAD_DELAY),
        # tránh cảnh báo deprecated trên Scrapy 2.19+
    },
}
