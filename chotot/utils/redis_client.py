"""Kết nối Redis dùng chung cho pipeline, settings và script.

Cấu hình đọc từ .env:
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD
"""

import os
from functools import lru_cache
from pathlib import Path
from urllib.parse import quote

import redis
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")


def redis_config():
    return {
        "host": os.getenv("REDIS_HOST", "localhost"),
        "port": int(os.getenv("REDIS_PORT", 6379)),
        "db": int(os.getenv("REDIS_DB", 0)),
        "password": os.getenv("REDIS_PASSWORD") or None,
    }


def redis_url():
    cfg = redis_config()
    auth = f":{quote(cfg['password'], safe='')}@" if cfg["password"] else ""
    return f"redis://{auth}{cfg['host']}:{cfg['port']}/{cfg['db']}"


@lru_cache(maxsize=1)
def get_redis():
    return redis.Redis(
        **redis_config(),
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
        health_check_interval=30,
    )
