"""Gửi message lên 4 topic Kafka (cấu trúc mỗi topic: chotot/hydra_topics.py):

    KAFKA_TOPIC_POST            hydra-chotot-post_lss-h             thông tin bài đăng   key: post_id
    KAFKA_TOPIC_POST_IMAGES     hydra-images-incoming         ảnh bài đăng         key: post_id
    KAFKA_TOPIC_PROFILE_BIO     hydra-profile-bio-chotot_lss-h      thông tin người đăng key: account_oid
    KAFKA_TOPIC_PROFILE_AVATAR  hydra-images-incoming-avatar  ảnh người đăng       key: account_oid

Topic nào cần gửi do pipeline chống trùng quyết định (item["topics"]). Chỉ đánh dấu chống trùng của 1 topic
SAU KHI Kafka xác nhận gửi thành công MỌI message của topic đó (vd đủ 3 ảnh của tin). Gửi lỗi, hoặc spider bị
tắt trước khi Kafka xác nhận, thì topic đó chưa được đánh dấu: lần chạy sau gửi lại riêng topic đó.
"""

import logging
import threading

from chotot.hydra_topics import GROUPS, message_key, topic_name
from chotot.pipelines.redis_dedup import all_groups, mark_sent
from chotot.utils.detail_test import DetailOutput, detail_spider
from chotot.utils.kafka_sender import make_sender

logger = logging.getLogger(__name__)


class HydraKafkaPipeline:
    def __init__(self, settings, stats=None, crawler=None):
        self.settings = settings
        self.topics = {group: topic_name(settings, group) for group in GROUPS}
        self.stats = stats
        self.crawler = crawler
        self.sender = None
        self.send_enabled = True
        self.detail_output = None  # luồng test detail_url: gom message để ghi file
        self.lock = threading.Lock()  # callback Kafka broker chạy ở thread của producer

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings, stats=crawler.stats, crawler=crawler)

    def _inc(self, name, group=None):
        if self.stats:
            self.stats.inc_value(f"kafka/{name}")
            if group:
                self.stats.inc_value(f"kafka/{group}/{name}")

    def open_spider(self, spider=None):
        detail = detail_spider(self.crawler)
        if detail is not None:
            self.detail_output = DetailOutput(
                self.settings.get("LOG_DIR", "logs"), detail.detail_output, topics=self.topics.values()
            )
            self.send_enabled = detail.detail_send_kafka
            if not self.send_enabled:
                logger.info("[DETAIL-TEST] Không gửi Kafka, chỉ ghi message ra file (thêm -a send_kafka=1 để gửi)")
                return

        self.sender = make_sender(self.settings)
        logging.getLogger("kafka").setLevel(logging.ERROR)
        mode = self.settings.get("KAFKA_MODE", "broker")
        if self.settings.getbool("KAFKA_LOCAL"):
            mode = f"local {','.join(self.settings.getlist('KAFKA_BOOTSTRAP_SERVERS'))}"
        logger.info("Kafka (%s) topic output: %s", mode, ", ".join(self.topics.values()))

    def process_item(self, item, spider=None):
        if "item_data" not in item:
            return item
        # Không có pipeline chống trùng phía trước: gửi đủ 4 topic
        topics = item.get("topics") or all_groups(item["item_data"])

        if self.detail_output is not None:
            for group in GROUPS:
                for message in topics.get(group, {}).get("messages", []):
                    self.detail_output.add(self.topics[group], message)
            if not self.send_enabled:
                return item

        if self.sender is None:
            self.sender = make_sender(self.settings)

        for group in GROUPS:
            entry = topics.get(group) or {}
            messages = entry.get("messages", [])
            if not messages:
                continue
            # Đếm số message còn chờ Kafka xác nhận; đủ hết (không lỗi) mới đánh dấu chống trùng
            tracker = {"left": len(messages), "failed": False, "mark": entry.get("mark")}
            for message in messages:
                key = message_key(group, message)
                self._inc("sent", group)
                self.sender.send(
                    self.topics[group],
                    message,
                    key=key,
                    on_success=lambda g=group, t=tracker: self._on_success(g, t),
                    on_error=lambda exc, g=group, t=tracker, k=key: self._on_error(g, t, k, exc),
                )
        return item

    def _on_success(self, group, tracker):
        self._inc("delivered", group)
        with self.lock:
            tracker["left"] -= 1
            done = tracker["left"] == 0 and not tracker["failed"]
        if done and tracker["mark"]:
            try:
                mark_sent(tracker["mark"])
                self._inc("marked", group)
            except Exception as e:  # Redis lỗi: lần sau gửi lại, không mất dữ liệu
                logger.error("Không ghi được dấu chống trùng %s: %s", tracker["mark"]["key"], e)

    def _on_error(self, group, tracker, key, exc):
        self._inc("failed", group)
        with self.lock:
            tracker["left"] -= 1
            tracker["failed"] = True  # không đánh dấu: lần sau gửi lại cả topic này của tin / người bán
        logger.error("[HYDRA-MONITOR] Gửi Kafka thất bại topic %s, key %s - %s", self.topics[group], key, exc)

    def close_spider(self, spider=None):
        if self.sender is not None:
            self.sender.close()  # http: gửi nốt các message còn trong lô
            logger.info("Đã đóng Kafka sender")
        self.sender = None
        if self.detail_output is not None:
            self.detail_output.write()
