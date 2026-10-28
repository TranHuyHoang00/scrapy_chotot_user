import logging
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from scrapy import signals
from scrapy.exceptions import NotConfigured

from chotot.hydra_topics import GROUPS, topic_name
from chotot.parsers import SITES, site_setting
from chotot.utils.hydra import get_ad

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
FILE_FORMAT = "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
NOISY_LOGGERS = ("kafka",)
REASONS = {
    "finished": "hết URL cần crawl",
    "closespider_itemcount": "đủ số item mục tiêu",
    "closespider_timeout": "hết thời gian cho phép",
    "shutdown": "người dùng dừng",
    "phone_token_error": "không lấy được token API phone",
}


def mask_proxy(proxy):
    if not proxy:
        return "không dùng"
    parsed = urlparse(proxy)
    return f"{parsed.scheme}://***:{parsed.port}" if parsed.port else "***"


def mask_url(url):
    return re.sub(r"//[^@/]*@", "//***@", url or "")


class CrawlMonitor:
    def __init__(self, crawler, log_dir, progress_every):
        self.crawler = crawler
        self.settings = crawler.settings
        self.stats = crawler.stats
        self.log_dir = Path(log_dir)
        self.progress_every = progress_every
        self.logger = logging.getLogger("monitor")
        self.handler = None
        self.log_file = None
        self.started = None
        self.items = 0

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        if not settings.getbool("MONITOR_ENABLED", True):
            raise NotConfigured
        ext = cls(
            crawler,
            settings.get("LOG_DIR", "logs"),
            settings.getint("MONITOR_PROGRESS_EVERY", 50),
        )
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(ext.item_dropped, signal=signals.item_dropped)
        return ext

    def _open_file_log(self, spider):
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.log_file = self.log_dir / f"{spider.name}_{datetime.now():%Y%m%d_%H%M%S}.log"
        self.handler = logging.FileHandler(self.log_file, encoding="utf-8")
        self.handler.setFormatter(logging.Formatter(FILE_FORMAT, DATE_FORMAT))
        self.handler.setLevel(self.settings.get("LOG_LEVEL", "INFO"))
        logging.getLogger().addHandler(self.handler)
        for name in NOISY_LOGGERS:
            logging.getLogger(name).setLevel(logging.WARNING)

    def _close_file_log(self):
        if self.handler:
            logging.getLogger().removeHandler(self.handler)
            self.handler.close()
            self.handler = None

    def spider_opened(self, spider):
        self.started = time.time()
        self._open_file_log(spider)
        s = self.settings
        target = s.getint("CLOSESPIDER_ITEMCOUNT")
        proxy = mask_proxy(s.get("CRAWL_PROXY")) if s.getbool("USE_PROXY") else "không dùng"

        self.logger.info("===== Bắt đầu spider '%s' =====", spider.name)
        self.logger.info("Ghi log vào: %s", self.log_file)
        detail_urls = getattr(spider, "detail_urls", None)
        if detail_urls:
            self.logger.info(
                "Chế độ: TEST detail_url (%d tin), %s, bỏ qua chống trùng",
                len(detail_urls),
                "CÓ gửi Kafka" if getattr(spider, "detail_send_kafka", False) else "không gửi Kafka, chỉ ghi file",
            )
        sites = [site for site in SITES if s.getbool(site_setting(site), True)]
        off = [site for site in SITES if site not in sites]
        self.logger.info("Site crawl: %s%s", ", ".join(sites) or "(không có)", f" | TẮT: {', '.join(off)}" if off else "")
        self.logger.info("Mục tiêu: %s", f"{target} item hợp lệ" if target else "không giới hạn")
        self.logger.info("Proxy: %s", proxy)
        self.logger.info(
            "Tốc độ: delay %ss, %s request song song/domain",
            s.getfloat("DOWNLOAD_DELAY"),
            s.getint("CONCURRENT_REQUESTS_PER_DOMAIN"),
        )
        self.logger.info("Redis: %s", mask_url(s.get("REDIS_URL")))
        self.logger.info(
            "API phone: %s",
            f"bật, cache {s.getint('PHONE_CACHE_TTL_HOURS')} giờ" if s.getbool("PHONE_API_ENABLED") else "tắt",
        )
        self.logger.info(
            "Trang người bán: %s",
            f"bật, cache {s.getint('SELLER_PROFILE_CACHE_TTL_HOURS')} giờ"
            if s.getbool("SELLER_PROFILE_ENABLED") else "tắt",
        )
        self.logger.info(
            "API số liệu người bán: %s",
            f"bật, cache {s.getint('SELLER_STATS_CACHE_TTL_HOURS')} giờ" if s.getbool("SELLER_STATS_ENABLED") else "tắt",
        )
        self.logger.info(
            "Kafka: %s",
            f"LOCAL {','.join(s.getlist('KAFKA_BOOTSTRAP_SERVERS'))} (KAFKA_LOCAL=true)"
            if s.getbool("KAFKA_LOCAL") else s.get("KAFKA_MODE"),
        )
        self.logger.info(
            "Kafka topic: input %s → output %s, %s, %s, %s", s.get("KAFKA_TOPIC_INPUT"),
            s.get("KAFKA_TOPIC_POST"), s.get("KAFKA_TOPIC_POST_IMAGES"),
            s.get("KAFKA_TOPIC_PROFILE_BIO"), s.get("KAFKA_TOPIC_PROFILE_AVATAR"),
        )

    def item_scraped(self, item, response, spider):
        self.items += 1
        site = get_ad(item).get("site") or "unknown"
        self.stats.inc_value(f"monitor/site/{site}")

        if self.progress_every and self.items % self.progress_every == 0:
            target = self.settings.getint("CLOSESPIDER_ITEMCOUNT")
            minutes = max((time.time() - self.started) / 60, 1 / 60)
            self.logger.info(
                "Tiến độ: %d/%s item hợp lệ (%.1f item/phút)",
                self.items, target or "?", self.items / minutes,
            )

    def item_dropped(self, item, response, exception, spider):
        self.logger.debug("Bỏ item: %s", exception)

    def _get(self, key):
        return self.stats.get_value(key, 0)

    def _with_prefix(self, prefix):
        return sorted(
            (k[len(prefix):], v) for k, v in self.stats.get_stats().items() if k.startswith(prefix)
        )

    def spider_closed(self, spider, reason):
        elapsed = time.time() - (self.started or time.time())
        target = self.settings.getint("CLOSESPIDER_ITEMCOUNT")
        valid = self._get("item_scraped_count")
        sent, delivered, failed = self._get("kafka/sent"), self._get("kafka/delivered"), self._get("kafka/failed")
        forbidden = self._get("robotstxt/forbidden")

        self.logger.info(
            "===== Kết thúc spider '%s' sau %.1fs (%s) =====",
            spider.name, elapsed, REASONS.get(reason, reason),
        )
        self.logger.info(
            "Request: %d gửi, %d phản hồi, %d thử lại, %d bị robots.txt chặn",
            self._get("downloader/request_count"), self._get("response_received_count"),
            self._get("retry/count"), forbidden,
        )
        self.logger.info("Item: %d hợp lệ, %d bị loại", valid, self._get("validation/dropped"))
        for name, count in self._with_prefix("validation/dropped/"):
            self.logger.info("  - Loại vì %s: %d", name, count)
        self.logger.info(
            "Chống trùng tin: %d mới, %d cập nhật, %d trùng",
            self._get("dedup/new"), self._get("dedup/updated"), self._get("dedup/duplicate"),
        )
        self.logger.info("Kafka: %d gửi, %d thành công, %d thất bại", sent, delivered, failed)
        for group in GROUPS:
            self.logger.info(
                "  - %s: %d gửi, %d thành công, %d thất bại (bỏ qua vì không đổi: %d)",
                topic_name(self.settings, group), self._get(f"kafka/{group}/sent"),
                self._get(f"kafka/{group}/delivered"), self._get(f"kafka/{group}/failed"),
                self._get(f"dedup/{group}/skip"),
            )
        if self.settings.getbool("PHONE_API_ENABLED"):
            self.logger.info(
                "API phone: %d gọi, %d có số, %d không có số, %d ẩn số, %d lỗi, %d lấy từ cache",
                self._get("phone_api/request"), self._get("phone_api/ok"), self._get("phone_api/no_phone"),
                self._get("phone_api/hidden"), self._get("phone_api/error"), self._get("phone_api/cache_hit"),
            )
            self.logger.info(
                "Token API phone: %d lần lấy thành công, %d lỗi, %d lần làm mới",
                self._get("phone_token/ok"), self._get("phone_token/error"), self._get("phone_token/renew"),
            )

        if self.settings.getbool("SELLER_PROFILE_ENABLED"):
            self.logger.info(
                "Trang người bán: %d tải, %d có dữ liệu, %d không có dữ liệu, %d lỗi, %d lấy từ cache",
                self._get("seller_profile/request"), self._get("seller_profile/ok"),
                self._get("seller_profile/no_data"), self._get("seller_profile/error"),
                self._get("seller_profile/cache_hit"),
            )

        if self.settings.getbool("SELLER_STATS_ENABLED"):
            self.logger.info(
                "API số liệu người bán: %d người bán, %d có dữ liệu, %d lỗi mọi API, %d lấy từ cache",
                self._get("seller_stats/request"), self._get("seller_stats/ok"),
                self._get("seller_stats/error"), self._get("seller_stats/cache_hit"),
            )
            for name, count in self._with_prefix("seller_stats/error/"):
                self.logger.info("  - API %s lỗi: %d", name, count)

        skipped_sites = self._with_prefix("chotot/skipped_site/")
        if skipped_sites:
            self.logger.info("Bỏ qua tin của site đã tắt: %s", ", ".join(f"{name}: {count}" for name, count in skipped_sites))

        categories = self._with_prefix("monitor/site/")
        if categories:
            self.logger.info("Theo site:")
            for name, count in categories:
                self.logger.info("  - %s: %d", name, count)

        if reason == "phone_token_error":
            self.logger.error(
                "Spider dừng vì không lấy được token API phone: kiểm tra PHONE_TOKEN_API_URL, PHONE_TOKEN_API_KEY"
            )
        if forbidden:
            self.logger.warning("Có %d URL bị robots.txt chặn, kiểm tra lại nguồn URL", forbidden)
        if sent != delivered + failed:
            self.logger.warning("Còn %d message Kafka chưa có kết quả", sent - delivered - failed)
        if failed:
            self.logger.warning("Có %d message gửi Kafka thất bại, sẽ được gửi lại ở lần crawl sau", failed)
        if target and valid < target:
            self.logger.warning("Chưa đạt mục tiêu: %d/%d item hợp lệ", valid, target)
        if not valid:
            self.logger.error("Không thu được item hợp lệ nào")

        self.logger.info("Log đầy đủ: %s", self.log_file)
        self._close_file_log()