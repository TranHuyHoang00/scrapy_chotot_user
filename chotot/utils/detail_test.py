"""Luồng test 1 (hoặc vài) tin cụ thể: ghi ra file JSON đúng các message gửi lên 4 topic Kafka.

    scrapy crawl chotot -a detail_url=https://xe.chotot.com/.../135193883.htm
    scrapy crawl chotot -a detail_url=<url1>,<url2>            # nhiều tin, cách nhau dấu phẩy
    scrapy crawl chotot -a detail_url=<url> -a output=logs/a.json   # tự đặt tên file
    scrapy crawl chotot -a detail_url=<url> -a send_kafka=1    # ghi file VÀ gửi Kafka thật

Luồng: trang chi tiết tin → trang người bán → API số liệu người bán → API phone → normalize → validation
→ (bỏ qua chống trùng) → dựng message 4 topic → ghi file. Không đọc trang danh sách, không đọc Kafka input.

- Mặc định KHÔNG gửi Kafka, KHÔNG gửi log [HYDRA-MONITOR] lên Kafka.
- Luôn bỏ qua chống trùng Redis: không kiểm tra, không đánh dấu. Test lại bao nhiêu lần cũng ra item,
  và không ảnh hưởng dữ liệu chống trùng của lần chạy thật.
- Vẫn dùng cache Redis của số điện thoại / trang người bán (như chạy thật).
- File: logs/detail_<ad_id>_<thời gian>.json, dạng {"<topic>": [message, ...], ...} đủ 4 topic
  (KAFKA_TOPIC_POST, _POST_IMAGES, _PROFILE_BIO, _PROFILE_AVATAR). Nhiều tin: message của các tin nối vào cùng list.
  Tin bị loại (validation) hoặc lỗi trang thì không có trong file, xem lý do trong log.
"""

import json
import logging
import re
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


def parse_detail_urls(value):
    """'url1, url2' → ['url1', 'url2'] (bỏ trùng, chỉ giữ http/https)."""
    if not value:
        return []
    urls = []
    for url in re.split(r"[,\s]+", str(value)):
        url = url.strip()
        if url.startswith(("http://", "https://")) and url not in urls:
            urls.append(url)
    return urls


def parse_flag(value):
    return str(value or "").strip().lower() in ("1", "true", "yes", "on")


def detail_spider(crawler):
    """Spider đang chạy ở chế độ test detail_url, hoặc None."""
    spider = getattr(crawler, "spider", None) if crawler else None
    return spider if getattr(spider, "detail_urls", None) else None


class DetailOutput:
    """Gom message Kafka (theo topic) của luồng test, ghi ra file khi spider đóng."""

    def __init__(self, log_dir, output=None, topics=()):
        self.log_dir = Path(log_dir)
        self.output = output
        self.messages = {topic: [] for topic in topics}  # topic → [message]; giữ thứ tự topic
        self.post_ids = []

    def add(self, topic, message):
        self.messages.setdefault(topic, []).append(message)
        post_id = message.get("post_id")
        if post_id and post_id not in self.post_ids:
            self.post_ids.append(post_id)

    def path(self):
        if self.output:
            return Path(self.output)
        name = "detail"
        if len(self.post_ids) == 1:
            name += f"_{self.post_ids[0]}"
        return self.log_dir / f"{name}_{datetime.now():%Y%m%d_%H%M%S}.json"

    def write(self):
        if not any(self.messages.values()):
            logger.warning("[DETAIL-TEST] Không có item nào để ghi: tin bị loại hoặc tải trang lỗi, xem log phía trên")
            return None
        path = self.path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.messages, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        counts = ", ".join(f"{topic}: {len(msgs)}" for topic, msgs in self.messages.items())
        logger.info("[DETAIL-TEST] Đã ghi %d tin vào: %s (%s)", len(self.post_ids), path, counts)
        return path
