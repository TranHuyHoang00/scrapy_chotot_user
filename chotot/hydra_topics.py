"""Dựng message cho 4 topic Kafka từ 1 tin đã crawl (AdItem nội bộ, có seller đầy đủ).

    nhóm            topic (.env)                                             message
    post            KAFKA_TOPIC_POST           hydra-chotot-post_lss-h             1 / tin
    post_images     KAFKA_TOPIC_POST_IMAGES    hydra-images-incoming         1 / ảnh của tin
    profile_bio     KAFKA_TOPIC_PROFILE_BIO    hydra-profile-bio-chotot_lss-h      1 / người bán
    profile_avatar  KAFKA_TOPIC_PROFILE_AVATAR hydra-images-incoming-avatar  1 / người bán có avatar

Cấu trúc, thứ tự key đúng theo file mẫu của từng topic. Field không lấy được từ dữ liệu crawl / API dùng giá trị
cố định trong mẫu (bên Kafka cấp để nhận dạng dữ liệu), ghi thẳng ở đây, KHÔNG đặt trong .env.
Thời gian là timestamp mili giây.
"""

import hashlib

from itemadapter import ItemAdapter

from chotot.items import AdItem
from chotot.schema import conform_item
from chotot.utils.hydra import iso_to_ms, now_ms

# ---------------------------------------------------------------- giá trị cố định (theo topic mẫu)

TENANT_ID = "9c70933aff6b2a6d08c687a6cbb6b765"
TENANT_IDS = [TENANT_ID]
TENANTS = [{"id": TENANT_ID, "name": "HY Global", "project": "global", "category_ids": []}]
OWNER_ID = "6aca663149b2732717abdd050062fe6e"
OWNER_NAME = "Chợ tốt"
OWNER_TYPE = "chotot"
CHANNEL_NAME = "market"
CRAWLER_SYSTEM = "api-crawler"
POST_TYPE = "post"
POST_DOC_TYPE = "Post"
IMAGE_TYPE_PHOTO = "photo"
IMAGE_TYPE_AVATAR = "avatar"
AVATAR_KEY_PREFIX = "chotot_avatar/"
# profile_bio: dùng khi trang người bán không có dữ liệu
BIO_DEFAULT_ABOUT = ""
BIO_DEFAULT_GENDER = "FEMALE"
BIO_PROFILE_TYPE = "person"

# ---------------------------------------------------------------- nhóm / topic

POST = "post"
POST_IMAGES = "post_images"
PROFILE_BIO = "profile_bio"
PROFILE_AVATAR = "profile_avatar"

# nhóm → (biến settings chứa tên topic, tên topic mặc định)
TOPIC_SETTINGS = {
    POST: ("KAFKA_TOPIC_POST", "hydra-chotot-post_lss-h"),
    POST_IMAGES: ("KAFKA_TOPIC_POST_IMAGES", "hydra-images-incoming"),
    PROFILE_BIO: ("KAFKA_TOPIC_PROFILE_BIO", "hydra-profile-bio-chotot_lss-h"),
    PROFILE_AVATAR: ("KAFKA_TOPIC_PROFILE_AVATAR", "hydra-images-incoming-avatar"),
}
GROUPS = tuple(TOPIC_SETTINGS)


def topic_name(settings, group):
    setting, default = TOPIC_SETTINGS[group]
    return settings.get(setting) or default


# ---------------------------------------------------------------- chuẩn hóa AdItem

LIST_FIELDS = ("images", "videos")
DICT_FIELDS = ("specs", "specs_labels", "location", "seller")
TIME_FIELDS = ("posted_at", "updated_at", "crawled_at")  # đã là ms từ parser; iso_to_ms phòng chuỗi ISO

# data của topic post: lấy từ AdItem, bỏ url, images, seller; thêm account_id, account_oid, attachments
POST_DATA_FIELDS = (
    "source", "site", "ad_id", "internal_ad_id", "account_id", "account_oid",
    "title", "description", "category", "category_name", "condition",
    "price", "price_text", "salary_min", "salary_max", "salary_type",
    "location", "posted_at", "updated_at", "crawled_at",
    "attachments", "videos", "specs", "specs_labels",
)
# 1 phần tử data.attachments (field không có dữ liệu để "" như mẫu)
ATTACHMENT_EMPTY = ("width", "ratio", "height", "target_id", "duration", "target_url", "description", "format")

# bio: field lấy nguyên từ seller (thứ tự theo mẫu)
BIO_SELLER_FIELDS = (
    "url", "name", "avatar", "is_company", "phone", "banner", "address",
    "joined_at", "joined_duration", "profile_updated_at", "is_active", "phone_verified", "email_verified",
    "followers", "followings", "rating", "total_rating", "total_rating_as_seller", "total_rating_as_buyer",
    "sold_ads", "live_ads", "response_rate", "response_rate_text", "response_time", "response_time_text",
    "company_name", "company_logo",
)


def build_data(ad):
    """AdItem → dict đủ trường, list/dict rỗng thay None, thời gian là ms, seller/location đủ key."""
    adapter = ItemAdapter(ad)
    data = {}
    for field in AdItem.fields:
        value = adapter.get(field)
        if value is None and field in LIST_FIELDS:
            value = []
        if value is None and field in DICT_FIELDS:
            value = {}
        if field in TIME_FIELDS:
            value = iso_to_ms(value)
        data[field] = value
    conform_item(data)
    return data


def _times(data, current):
    """(created_time, flag_crawl_created_time, updated_time) của tin."""
    created_time = data.get("posted_at")
    flag = 1 if isinstance(created_time, int) else 0
    if not flag:
        created_time = current
    updated_time = data.get("updated_at")
    if not isinstance(updated_time, int):
        updated_time = current
    return created_time, flag, updated_time


def post_entry_rowkey(post_id, account_oid):
    """entry_rowkey của bài đăng = md5 (hex) của "<post_id>_<account_oid>",
    vd md5("135401339_b9f3fc8c7281753d9c0276e16ec33d31") = "a7b22f86dde3dd42eb157bdd2fb1c2d2".
    Tin không có account_oid: md5("<post_id>"). Dùng chung cho ảnh bài đăng (entry_rowkey, attachments.entry_id)."""
    raw = f"{post_id}_{account_oid}" if account_oid else f"{post_id}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _attachment(url, entry_rowkey):
    item = {key: "" for key in ATTACHMENT_EMPTY}
    item.update(url=url, entry_id=entry_rowkey, type=IMAGE_TYPE_PHOTO)
    order = ("width", "url", "ratio", "entry_id", "height", "target_id", "duration",
             "type", "target_url", "description", "format")
    return {key: item[key] for key in order}


# ---------------------------------------------------------------- từng topic

def build_post(data, post_id, url, current):
    seller = data["seller"]
    entry_rowkey = post_entry_rowkey(post_id, seller.get("account_oid"))
    created_time, flag, updated_time = _times(data, current)

    post_data = dict(data)
    post_data["account_id"] = seller.get("account_id")
    post_data["account_oid"] = seller.get("account_oid")
    post_data["attachments"] = [_attachment(u, entry_rowkey) for u in data.get("images") or []]
    post_data = {key: post_data.get(key) for key in POST_DATA_FIELDS}

    return {
        "url": data.get("url") or url,
        "owner_name": OWNER_NAME,
        "owner_id": OWNER_ID,
        "updated_time": updated_time,
        "tenant_ids": list(TENANT_IDS),
        "inserted_time": current,
        "post_id": post_id,
        "created_time": created_time,
        "flag_crawl_created_time": flag,
        "entry_rowkey": entry_rowkey,
        "type": POST_TYPE,
        "docType": POST_DOC_TYPE,
        "crawler_system": CRAWLER_SYSTEM,
        "_tenants": [dict(t) for t in TENANTS],
        "owner_type": OWNER_TYPE,
        "data": post_data,
        "author_id": seller.get("account_oid"),
        "channel_name": CHANNEL_NAME,
        "author_name": seller.get("name"),
    }


def build_post_images(post):
    """1 message / ảnh (không gửi video). Thời gian, entry_rowkey theo tin."""
    messages = []
    for attachment in post["data"]["attachments"]:
        messages.append({
            "url": attachment["url"],
            "media_url": attachment["url"],
            "image_type": IMAGE_TYPE_PHOTO,
            "updated_time": post["updated_time"],
            "tenant_ids": list(TENANT_IDS),
            "post_id": post["post_id"],
            "created_time": post["created_time"],
            "entry_rowkey": post["entry_rowkey"],
            "profile_id": OWNER_ID,
            "tenants": [dict(t) for t in TENANTS],
            "channel_name": CHANNEL_NAME,
        })
    return messages


def normalize_gender(value):
    """Giới tính trên trang người bán → "MALE" / "FEMALE"; rỗng → None (builder dùng giá trị mẫu)."""
    text = str(value or "").strip().lower()
    if not text:
        return None
    if text in ("m", "male", "nam", "man", "1"):
        return "MALE"
    if text in ("f", "female", "nữ", "nu", "woman", "2"):
        return "FEMALE"
    return text.upper()


def build_profile_bio(seller):
    """1 message / người bán. Không có account_oid thì None."""
    oid = seller.get("account_oid")
    if not oid:
        return None
    message = {
        "doc_id": oid,
        "entry_rowkey": oid,
        "profile_id": oid,
        "about": seller.get("about") or BIO_DEFAULT_ABOUT,
        "gender": normalize_gender(seller.get("gender")) or BIO_DEFAULT_GENDER,
        "channel_name": CHANNEL_NAME,
        "profile_type": BIO_PROFILE_TYPE,
    }
    message.update({field: seller.get(field) for field in BIO_SELLER_FIELDS})
    return message


def build_profile_avatar(seller, crawled_at):
    """1 message / người bán có avatar. Không có account_oid hoặc avatar thì None.
    created_time / updated_time = thời điểm crawl."""
    oid = seller.get("account_oid")
    avatar = seller.get("avatar")
    if not oid or not avatar:
        return None
    return {
        "url": avatar,
        "media_url": avatar,
        "image_type": IMAGE_TYPE_AVATAR,
        "updated_time": crawled_at,
        "tenant_ids": list(TENANT_IDS),
        "image_key": f"{AVATAR_KEY_PREFIX}{oid}",
        "created_time": crawled_at,
        "entry_rowkey": oid,
        "profile_id": oid,
        "channel_name": CHANNEL_NAME,
    }


def build_messages(item_data):
    """item_data (envelope: url, post_id, data=AdItem) → {nhóm: [message, ...]} cho cả 4 topic."""
    data = build_data(item_data["data"])
    current = now_ms()
    post = build_post(data, item_data["post_id"], item_data.get("url"), current)
    seller = data["seller"]
    crawled_at = data.get("crawled_at") if isinstance(data.get("crawled_at"), int) else current
    bio = build_profile_bio(seller)
    avatar = build_profile_avatar(seller, crawled_at)
    return {
        POST: [post],
        POST_IMAGES: build_post_images(post),
        PROFILE_BIO: [bio] if bio else [],
        PROFILE_AVATAR: [avatar] if avatar else [],
    }


def message_key(group, message):
    """Key Kafka: post_id cho bài đăng / ảnh bài đăng, account_oid cho người bán / avatar."""
    if group in (POST, POST_IMAGES):
        return message.get("post_id")
    return message.get("profile_id")
