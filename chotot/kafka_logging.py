"""Gửi log có tag [HYDRA-MONITOR] sang Kafka (topic hydra-spider-log), giống spider mẫu."""

import logging
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from chotot.utils.kafka_sender import make_sender

MONITOR_TAG = "HYDRA-MONITOR"
LOG_TZ = ZoneInfo("Asia/Bangkok")


class KafkaLoggingHandler(logging.Handler):
    def __init__(self, settings):
        super().__init__()
        self.topic = settings.get("KAFKA_TOPIC_LOG", "hydra-spider-log")
        self.spider_name = settings.get("BOT_NAME")
        self.task_id = os.environ.get("CRAWLAB_TASK_ID")
        self.sender = None
        try:
            self.sender = make_sender(settings)
        except Exception as e:  # không có Kafka thì spider vẫn chạy, chỉ mất log giám sát
            sys.stderr.write(f"KafkaLoggingHandler: không kết nối được Kafka: {e}\n")

    def emit(self, record):
        if self.sender is None:
            return
        try:
            message = self.format(record)
            if MONITOR_TAG not in message:
                return
            formatted_time = datetime.fromtimestamp(record.created, LOG_TZ).strftime("%Y-%m-%d %H:%M:%S")
            self.sender.send(
                self.topic,
                {
                    "msg": f"{formatted_time} [{self.spider_name}] {record.levelname}: {message}",
                    "level": record.levelname,
                    "message": message,
                    "asctime": formatted_time,
                    "name": record.name,
                    "spider_name": self.spider_name,
                    "task_id": self.task_id,
                },
            )
        except Exception:
            self.handleError(record)

    def close(self):
        try:
            if self.sender is not None:
                self.sender.close()
        except Exception:
            pass
        self.sender = None
        super().close()
