"""Tạo Kafka producer/consumer dùng chung, hỗ trợ SSL giống các spider Hydra.

Mọi thông số đọc từ Scrapy settings (xem chotot/settings.py):
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_SECURITY_PROTOCOL,
    KAFKA_SSL_CAFILE, KAFKA_SSL_KEYFILE, KAFKA_SSL_CERTFILE,
    KAFKA_SSL_CHECK_HOSTNAME, KAFKA_API_VERSION
"""

import json

# Thư viện kafka chỉ import khi tạo producer/consumer (chế độ broker),
# để KAFKA_MODE=http không phụ thuộc vào nó.


def _api_version(settings):
    raw = (settings.get("KAFKA_API_VERSION") or "").strip()
    if not raw:
        return None
    return tuple(int(x) for x in raw.split("."))


def connection_config(settings):
    """Thông số kết nối chung cho cả producer và consumer."""
    servers = settings.getlist("KAFKA_BOOTSTRAP_SERVERS") or ["localhost:9092"]
    config = {
        "bootstrap_servers": servers,
        "security_protocol": settings.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
    }
    if config["security_protocol"].upper().startswith("SSL"):
        config.update(
            ssl_cafile=settings.get("KAFKA_SSL_CAFILE"),
            ssl_keyfile=settings.get("KAFKA_SSL_KEYFILE"),
            ssl_certfile=settings.get("KAFKA_SSL_CERTFILE"),
            ssl_check_hostname=settings.getbool("KAFKA_SSL_CHECK_HOSTNAME", False),
        )
    api_version = _api_version(settings)
    if api_version:
        config["api_version"] = api_version
    return config


def _serialize_value(value):
    return json.dumps(value, ensure_ascii=False, default=str).encode("utf-8")


def _serialize_key(key):
    return str(key).encode("utf-8") if key is not None else None


def make_producer(settings):
    from kafka import KafkaProducer

    return KafkaProducer(
        **connection_config(settings),
        value_serializer=_serialize_value,
        key_serializer=_serialize_key,
        acks="all",
        retries=5,
        linger_ms=50,
        compression_type="gzip",
        request_timeout_ms=30000,
    )


def make_consumer(settings, group_id=None):
    from kafka import KafkaConsumer

    return KafkaConsumer(
        **connection_config(settings),
        group_id=group_id or settings.get("KAFKA_GROUP_ID"),
        auto_offset_reset=settings.get("KAFKA_AUTO_OFFSET_RESET", "earliest"),
    )


def close_producer(producer):
    if producer is None:
        return
    try:
        producer.flush()
    finally:
        producer.close()

