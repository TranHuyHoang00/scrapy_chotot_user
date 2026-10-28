"""Phần chung cho các trang bổ sung thông tin người bán (hiện: trang người bán, utils/seller_profile.py):
tải trang qua Scrapy, cache kết quả trong bộ nhớ (lượt chạy) và Redis, đếm thống kê, log lỗi.

Lớp con khai báo:
    name            tiền tố thống kê, vd "seller_profile" → seller_profile/ok, seller_profile/error
    label           tên hiển thị trong log, vd "Trang người bán"
    cache_prefix    tiền tố key Redis, vd "seller" → seller:<key>
    meta_key        key trong request.meta chứa định danh, vd "seller_oid"
    required_keys   các key bắt buộc trong cache; cache thiếu key (ghi bởi code cũ) thì tải lại
    enabled_setting, ttl_setting, default_ttl_hours
    parse(response, key) → dict info hoặc None
"""

import json
import logging

import scrapy

from chotot.utils.redis_client import get_redis

logger = logging.getLogger(__name__)


class PageCache:
    name = None
    label = None
    cache_prefix = None
    meta_key = None
    required_keys = frozenset()
    enabled_setting = None
    ttl_setting = None
    default_ttl_hours = 72
    # Field của seller coi là "chưa biết" (null do lỗi, không phải không có dữ liệu) khi trang / API lỗi.
    # Chống trùng dùng để không ghi đè giá trị tốt đã gửi trước đó bằng null (pipelines/redis_dedup.py)
    failure_fields = ()

    def __init__(self, cache_ttl_hours=None, timeout=30, crawler=None):
        hours = self.default_ttl_hours if cache_ttl_hours is None else cache_ttl_hours
        self.cache_ttl = max(0, int(hours)) * 3600
        self.timeout = timeout
        self.crawler = crawler
        self.memory = {}  # key → info, chỉ trong lượt chạy hiện tại

    @classmethod
    def from_crawler(cls, crawler):
        """None nếu <enabled_setting>=false."""
        s = crawler.settings
        if not s.getbool(cls.enabled_setting, True):
            return None
        return cls(
            cache_ttl_hours=s.getint(cls.ttl_setting, cls.default_ttl_hours),
            timeout=s.getint("DOWNLOAD_TIMEOUT", 30),
            crawler=crawler,
        )

    @property
    def stats(self):
        try:
            return self.crawler.stats if self.crawler else None
        except RuntimeError:  # Scrapy 2.19+: stats chưa có lúc tạo spider
            return None

    def _inc(self, name):
        if self.stats:
            self.stats.inc_value(f"{self.name}/{name}")

    # ----------------------------------------------------------------- cache

    def cache_key(self, key):
        return f"{self.cache_prefix}:{key}"

    def get_cached(self, key):
        """dict info nếu đã có (bộ nhớ hoặc Redis), None nếu phải tải trang."""
        if key in self.memory:
            self._inc("cache_hit")
            return self.memory[key]
        if not self.cache_ttl:
            return None
        try:
            raw = get_redis().get(self.cache_key(key))
        except Exception as e:  # Redis lỗi thì tải trang như bình thường
            logger.warning("Đọc cache %s lỗi: %s", self.cache_prefix, e)
            return None
        if not raw:
            return None
        try:
            info = json.loads(raw)
        except ValueError:
            return None
        if not isinstance(info, dict) or not self.required_keys.issubset(info):
            self._inc("cache_outdated")
            return None
        self.memory[key] = info
        self._inc("cache_hit")
        return info

    def set_cached(self, key, info):
        self.memory[key] = info
        if not self.cache_ttl:
            return
        try:
            get_redis().set(self.cache_key(key), json.dumps(info, ensure_ascii=False), ex=self.cache_ttl)
        except Exception as e:
            logger.warning("Ghi cache %s lỗi: %s", self.cache_prefix, e)

    # --------------------------------------------------------------- request

    def build_request(self, key, url, callback, errback, meta=None):
        meta = dict(meta or {})
        # Không đặt handle_httpstatus_all: cần Scrapy đi theo redirect. Lỗi 4xx/5xx đi vào errback
        meta[self.meta_key] = key
        self._inc("request")
        return scrapy.Request(
            url,
            callback=callback,
            errback=errback,
            meta=meta,
            dont_filter=True,  # đã chống tải trùng bằng key
        )

    def parse(self, response, key):
        raise NotImplementedError

    def unknown_fields(self, info):
        """Field "chưa biết" của 1 kết quả: lỗi hẳn (info None) thì failure_fields, lỗi 1 phần thì info["_unknown"]."""
        if info is None:
            return list(self.failure_fields)
        return list(info.get("_unknown") or [])

    def parse_response(self, response):
        """Trả về dict info (đã cache) hoặc None nếu trang lỗi / không có dữ liệu."""
        key = response.meta.get(self.meta_key)
        if response.meta.get("redirect_urls"):
            self._inc("redirected")
            logger.info("%s %s được chuyển hướng tới %s", self.label, key, response.url)
        if response.status != 200:
            self._inc(f"http_{response.status}")
            self._inc("error")
            logger.warning("%s %s trả về HTTP %s", self.label, key, response.status)
            return None

        info = self.parse(response, key)
        if info is None:
            self._inc("no_data")
            logger.warning("%s %s không có dữ liệu: %s", self.label, key, response.url)
            return None

        self._inc("ok")
        self.set_cached(key, info)
        return info

    def on_failure(self, failure):
        """Lỗi HTTP 4xx/5xx, lỗi mạng / timeout (sau khi đã retry)."""
        self._inc("error")
        response = getattr(getattr(failure, "value", None), "response", None)
        if response is not None:
            self._inc(f"http_{response.status}")
            reason = f"HTTP {response.status} ({response.url})"
        else:
            reason = failure.getErrorMessage()
        logger.warning("Tải %s %s lỗi: %s", self.label.lower(), failure.request.meta.get(self.meta_key), reason)
