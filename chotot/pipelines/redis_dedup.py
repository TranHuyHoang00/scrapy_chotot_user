"""Chống trùng RIÊNG TỪNG TOPIC (Redis). Dựng message 4 topic (chotot/hydra_topics.py) rồi quyết định topic nào
cần gửi; kết quả để trong item["topics"] = {nhóm: {"messages": [...], "mark": {...} | None}}.

Mỗi message đã gửi được nhớ bằng 1 "dấu vân tay" (sha1 nội dung) ở 1 key Redis riêng, có thời hạn:

    nhóm            key Redis                          gửi khi
    post            dedup:post:<site>:<post_id>         tin mới / nội dung tin đổi (chỉ "đẩy tin" thì không)
    post_images     dedup:images:<site>:<post_id>       cùng lúc với post (cùng dấu vân tay với post)
    profile_bio     dedup:bio:<account_oid>             thông tin người bán đổi (không tính joined_duration)
    profile_avatar  dedup:avatar:<account_oid>          link avatar đổi

1. Chỉ đánh dấu SAU KHI Kafka xác nhận gửi thành công mọi message của topic đó (kafka_pipeline gọi
   mark_sent). Spider bị tắt ngang / gửi lỗi thì chưa đánh dấu, lần sau gửi lại.
2. Key hết hạn sau DEDUP_TTL_DAYS ngày (mặc định 30, 0 = không hết hạn): quá hạn thì gửi lại 1 lần để đồng bộ,
   Redis tự dọn key cũ.
3. Không ghi đè dữ liệu tốt bằng null do lỗi: field người bán bị null vì trang / API lỗi (item_data["unknown"],
   spider ghi lại) thì bio dùng giá trị đã gửi lần trước (dedup:bio lưu cả message đã gửi).
4. Bio không tính joined_duration ("7 năm 4 tháng", tự đổi mỗi tháng) vào dấu vân tay.

- Nhiều tin cùng người bán trong 1 lượt chạy: bio / avatar chỉ gửi 1 lần (nhớ trong bộ nhớ lượt chạy).
- Không topic nào cần gửi: bỏ item (DropItem duplicate).
- Luồng test detail_url: không kiểm tra, không đánh dấu, gửi / ghi file đủ 4 topic.
"""

import hashlib
import json
import logging

from scrapy.exceptions import DropItem

from chotot.hydra_topics import GROUPS, POST, POST_IMAGES, PROFILE_AVATAR, PROFILE_BIO, build_messages
from chotot.utils.detail_test import detail_spider
from chotot.utils.redis_client import get_redis

# Bỏ qua khi so sánh tin: thời điểm gửi / crawl, và thời gian đẩy tin (tin chỉ được "đẩy lại" thì không gửi lại)
POST_IGNORED = ("inserted_time", "updated_time")
POST_DATA_IGNORED = ("crawled_at", "updated_at")
# Bỏ qua khi so sánh người bán: chuỗi web tự tính theo ngày ("7 năm 4 tháng")
BIO_IGNORED = ("joined_duration",)

logger = logging.getLogger(__name__)


def _sha1(value):
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def post_fingerprint(post):
    data = {k: v for k, v in post["data"].items() if k not in POST_DATA_IGNORED}
    rest = {k: v for k, v in post.items() if k not in POST_IGNORED and k != "data"}
    return _sha1({"data": data, **rest})


def bio_fingerprint(bio):
    return _sha1({k: v for k, v in bio.items() if k not in BIO_IGNORED})


def dedup_key(group, message, site=None):
    if group == POST:
        return f"dedup:post:{site}:{message['post_id']}"
    if group == POST_IMAGES:
        return f"dedup:images:{site}:{message['post_id']}"
    if group == PROFILE_BIO:
        return f"dedup:bio:{message['profile_id']}"
    return f"dedup:avatar:{message['profile_id']}"


def _stored_fp(raw):
    """Giá trị Redis → (dấu vân tay, message bio đã gửi | None). Bio lưu JSON {"fp", "msg"}, nhóm khác lưu fp."""
    if not raw:
        return None, None
    if raw.startswith("{"):
        try:
            stored = json.loads(raw)
            return stored.get("fp"), stored.get("msg")
        except ValueError:
            return None, None
    return raw, None


def mark_sent(mark):
    """Đánh dấu đã gửi (gọi sau khi Kafka xác nhận gửi thành công mọi message của nhóm)."""
    if mark:
        get_redis().set(mark["key"], mark["value"], ex=mark.get("ttl") or None)


def all_groups(item_data):
    """Dựng message 4 topic, gửi hết, không đánh dấu (luồng test / không có chống trùng)."""
    return {group: {"messages": msgs, "mark": None} for group, msgs in build_messages(item_data).items()}


class RedisDedupPipeline:
    def __init__(self, allow_updates=True, ttl_days=30, stats=None, crawler=None):
        self.allow_updates = allow_updates
        self.ttl = max(0, int(ttl_days)) * 86400
        self.stats = stats
        self.crawler = crawler
        self.redis = None
        # key → dấu vân tay đã quyết định gửi trong lượt chạy này (chưa chắc đã được Kafka xác nhận):
        # tin thứ 2 cùng người bán không gửi lại bio / avatar
        self.inflight = {}

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            allow_updates=crawler.settings.getbool("DEDUP_ALLOW_UPDATES", True),
            ttl_days=crawler.settings.getint("DEDUP_TTL_DAYS", 30),
            stats=crawler.stats,
            crawler=crawler,
        )

    def open_spider(self, spider=None):
        self.redis = get_redis()
        self.redis.ping()

    def _inc(self, name):
        if self.stats:
            self.stats.inc_value(f"dedup/{name}")

    def _carry_forward(self, bio, previous, unknown):
        """Field null do lỗi → giá trị đã gửi lần trước (nếu có). Trả về số field đã giữ lại."""
        if not previous or not unknown:
            return 0
        carried = 0
        for field in unknown:
            if field in bio and bio[field] is None and previous.get(field) is not None:
                bio[field] = previous[field]
                carried += 1
        return carried

    def process_item(self, item, spider=None):
        if "item_data" not in item:
            return item
        # Luồng test detail_url: không kiểm tra, không đánh dấu (không ảnh hưởng lần chạy thật)
        if detail_spider(self.crawler):
            self._inc("skipped_detail_test")
            item["topics"] = all_groups(item["item_data"])
            return item

        if self.redis is None:
            self.redis = get_redis()

        messages = build_messages(item["item_data"])
        unknown = item["item_data"].get("unknown") or []
        post = messages[POST][0]
        site = post["data"].get("site")
        post_fp = post_fingerprint(post)

        topics = {}
        for group in GROUPS:
            msgs = messages[group]
            if not msgs:
                topics[group] = {"messages": [], "mark": None}
                continue

            key = dedup_key(group, msgs[0], site)
            old_fp, old_msg = _stored_fp(self.redis.get(key))

            if group == PROFILE_BIO:
                carried = self._carry_forward(msgs[0], old_msg, unknown)
                if carried:
                    self._inc("bio_carried_fields")
                    logger.info("Người bán %s: giữ %d field đã gửi trước (lần này lỗi): %s",
                                msgs[0]["profile_id"], carried, ", ".join(unknown))
                fp = bio_fingerprint(msgs[0])
                value = json.dumps({"fp": fp, "msg": msgs[0]}, ensure_ascii=False, default=str)
            elif group in (POST, POST_IMAGES):
                fp = value = post_fp  # ảnh đi cùng tin, đánh dấu riêng để gửi lỗi thì gửi lại riêng ảnh
            else:
                fp = value = _sha1(msgs[0]["url"])

            send = old_fp != fp and self.inflight.get(key) != fp
            if group == POST and old_fp and not self.allow_updates:
                send = False
            if group == POST:
                self._inc("duplicate" if not send else ("updated" if old_fp else "new"))
            self._inc(f"{group}/{'send' if send else 'skip'}")

            if send:
                self.inflight[key] = fp
                topics[group] = {"messages": msgs, "mark": {"key": key, "value": value, "ttl": self.ttl}}
            else:
                topics[group] = {"messages": [], "mark": None}

        if not any(entry["messages"] for entry in topics.values()):
            raise DropItem(f"[duplicate] {site}:{post['post_id']}")
        item["topics"] = topics
        return item
