"""Gửi message lên Kafka theo 2 cách, chọn bằng KAFKA_MODE:

    broker  kết nối thẳng Kafka broker (môi trường Hydra, giống spider mẫu)
    http    gọi API publish-message (Kafka dev):
                POST KAFKA_HTTP_URL?apikey=KAFKA_HTTP_API_KEY
                {"topic": "...", "message": "<chuỗi JSON của mảng message>"}
            Message được gom theo lô KAFKA_HTTP_BATCH_SIZE để giảm số request.

Cả 2 có cùng giao diện: send(topic, value, key, on_success, on_error), flush(), close().
"""

import json
import logging
import threading
from collections import defaultdict

import requests

from chotot.utils.kafka_producer import close_producer, make_producer

# Không gắn tag HYDRA-MONITOR vào log của module này để tránh vòng lặp với KafkaLoggingHandler
logger = logging.getLogger(__name__)


class BrokerSender:
    def __init__(self, settings):
        self.producer = make_producer(settings)

    def send(self, topic, value, key=None, on_success=None, on_error=None):
        future = self.producer.send(topic, value=value, key=key)
        if on_success:
            future.add_callback(lambda _metadata: on_success())
        if on_error:
            future.add_errback(on_error)

    def flush(self):
        self.producer.flush()

    def close(self):
        close_producer(self.producer)
        self.producer = None


class HttpSender:
    def __init__(self, settings, batch_size=None):
        self.url = settings.get("KAFKA_HTTP_URL")
        self.api_key = settings.get("KAFKA_HTTP_API_KEY")
        if not self.url or not self.api_key:
            raise ValueError("KAFKA_MODE=http cần KAFKA_HTTP_URL và KAFKA_HTTP_API_KEY trong .env")
        self.batch_size = max(1, batch_size or settings.getint("KAFKA_HTTP_BATCH_SIZE", 50))
        self.timeout = settings.getint("KAFKA_HTTP_TIMEOUT", 30)
        self.verify = settings.getbool("KAFKA_HTTP_VERIFY_SSL", True)
        self.session = requests.Session()
        self.lock = threading.Lock()
        self.buffers = defaultdict(list)  # topic → [(value, on_success, on_error)]

    def send(self, topic, value, key=None, on_success=None, on_error=None):
        batch = None
        with self.lock:
            buffer = self.buffers[topic]
            buffer.append((value, on_success, on_error))
            if len(buffer) >= self.batch_size:
                batch = self.buffers.pop(topic)
        if batch:
            self._post(topic, batch)

    def flush(self):
        with self.lock:
            pending = dict(self.buffers)
            self.buffers.clear()
        for topic, batch in pending.items():
            self._post(topic, batch)

    def close(self):
        try:
            self.flush()
        finally:
            self.session.close()

    def _post(self, topic, batch):
        payload = {
            "topic": topic,
            "message": json.dumps([value for value, _, _ in batch], ensure_ascii=False, default=str),
        }
        try:
            res = self.session.post(
                self.url,
                params={"apikey": self.api_key},
                json=payload,
                timeout=self.timeout,
                verify=self.verify,
            )
            if not res.ok:
                raise RuntimeError(f"HTTP {res.status_code}: {res.text[:300]}")
        except Exception as e:
            logger.error("Gửi %d message lên topic %s qua API thất bại: %s", len(batch), topic, e)
            for _, _, on_error in batch:
                if on_error:
                    on_error(e)
            return
        logger.info("Đã gửi %d message lên topic %s qua API", len(batch), topic)
        for _, on_success, _ in batch:
            if on_success:
                on_success()


def make_sender(settings, batch_size=None):
    mode = (settings.get("KAFKA_MODE") or "broker").strip().lower()
    if mode == "http":
        return HttpSender(settings, batch_size=batch_size)
    if mode != "broker":
        raise ValueError(f"KAFKA_MODE không hợp lệ: {mode} (chỉ nhận broker hoặc http)")
    return BrokerSender(settings)
