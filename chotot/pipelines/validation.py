"""Loại tin thiếu dữ liệu. Trường bắt buộc cấu hình theo từng site (VALIDATION_RULES).

price không bắt buộc: tin cho tặng, thỏa thuận, không ghi giá vẫn giữ với price = null.
Giá sai kiểu hoặc âm thì đặt price = null thay vì loại tin.
"""

from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

from chotot.utils.hydra import get_ad

DEFAULT_RULES = {
    "default": ["ad_id", "url", "title"],
}
DEFAULT_MIN_TITLE_LENGTH = 3


class ValidationPipeline:
    def __init__(self, rules, min_title_length, stats=None):
        self.rules = rules
        self.min_title_length = min_title_length
        self.stats = stats

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        return cls(
            rules=settings.getdict("VALIDATION_RULES") or DEFAULT_RULES,
            min_title_length=settings.getint("VALIDATION_MIN_TITLE_LENGTH", DEFAULT_MIN_TITLE_LENGTH),
            stats=crawler.stats,
        )

    def _drop(self, reason, adapter):
        site = adapter.get("site") or "unknown"
        if self.stats:
            self.stats.inc_value(f"validation/dropped/{site}/{reason}")
            self.stats.inc_value("validation/dropped")
        raise DropItem(f"[{site}:{reason}] {adapter.get('url')}")

    def process_item(self, item, spider=None):
        adapter = ItemAdapter(get_ad(item))
        site = adapter.get("site")
        required = self.rules.get(site) or self.rules.get("default") or []

        missing = [f for f in required if adapter.get(f) in (None, "", [], {})]
        if missing:
            self._drop(f"missing_{missing[0]}", adapter)

        if not str(adapter["url"]).startswith(("http://", "https://")):
            self._drop("invalid_url", adapter)

        if len(adapter["title"]) < self.min_title_length:
            self._drop("short_title", adapter)

        price = adapter.get("price")
        if price is not None and (not isinstance(price, int) or isinstance(price, bool) or price < 0):
            adapter["price"] = None
            if self.stats:
                self.stats.inc_value("validation/price_reset")
        if adapter.get("price") is None and self.stats:
            self.stats.inc_value("validation/no_price")

        if self.stats:
            self.stats.inc_value("validation/valid")
        return item
