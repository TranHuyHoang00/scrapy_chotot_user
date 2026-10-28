"""Đẩy 1 profile test vào Kafka topic input (chỉ có ích với KAFKA_MODE=broker hoặc KAFKA_LOCAL=true).

Với Kafka dev qua API (KAFKA_MODE=http) spider không đọc được topic input,
hãy chạy thẳng: scrapy crawl chotot -a start_url=https://www.chotot.com/mua-ban

Trên hệ thống thật, profile do Hydra scheduler đẩy vào topic
hydra-profile-scheduler-chotot; không cần chạy script này.

Profile trỏ tới trang tổng hợp CHOTOT_START_URL (mặc định https://www.chotot.com/mua-ban).

Cách dùng (chạy tại thư mục gốc dự án):
    python scripts/seed_profiles.py                 # mặc định CHOTOT_MAX_PAGES trang
    python scripts/seed_profiles.py --pages 2       # chỉ crawl 2 trang danh sách
    python scripts/seed_profiles.py --dry-run       # chỉ in ra
"""

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)
os.environ.setdefault("SCRAPY_SETTINGS_MODULE", "chotot.settings")

from scrapy.utils.project import get_project_settings  # noqa: E402

from chotot.utils.hydra import make_test_profile  # noqa: E402
from chotot.utils.kafka_sender import make_sender  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description="Đẩy profile test vào Kafka topic input")
    parser.add_argument("--pages", type=int, help="số trang danh sách cần crawl")
    parser.add_argument("--items", type=int, default=0, help="số tin tối đa mỗi trang, 0 = lấy hết")
    parser.add_argument("--dry-run", action="store_true", help="chỉ in ra, không đẩy")
    args = parser.parse_args()

    settings = get_project_settings()
    topic = settings.get("KAFKA_TOPIC_INPUT")
    profile = make_test_profile(
        settings.get("CHOTOT_START_URL"),
        args.pages or settings.getint("CHOTOT_MAX_PAGES"),
        args.items,
    )

    if args.dry_run:
        print(json.dumps(profile, ensure_ascii=False, indent=2))
        print(f"\n[dry-run] profile sẽ được đẩy vào topic '{topic}'")
        return

    sender = make_sender(settings)
    sender.send(topic, profile, key=profile["profile_id"])
    sender.close()
    print(f"Đã đẩy profile {profile['url']} ({profile['max_pages']} trang) vào topic '{topic}'")


if __name__ == "__main__":
    main()
