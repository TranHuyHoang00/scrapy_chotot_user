"""Lấy thông tin trang người bán: https://www.chotot.com/user/<account_oid>

Trang chi tiết tin chỉ có tên, avatar... của người bán. Các thông tin dưới đây nằm ở trang người bán,
trong JSON __NEXT_DATA__ (props.pageProps.profile), dùng chung cho mọi site (chotot, xe, vieclamtot, nhatot):

    seller field          profile field                 ví dụ
    name                  full_name                     "siêu thị ô tô Nhật Bắc"   (*)
    avatar                avatar                        "https://cdn.chotot.com/..."  (*)
    banner                banner                        "https://cdn.chotot.com/uac_banner/31634981"
    address               address                       "QL3 Lộc Hà Mai Lâm Đông Anh"
    about                 description                   "Mua bán trao đổi tất cả smartphone cũ mới..."
    gender                gender                        thường "" (rỗng → null)
    joined_at             create_time / start_time (s)  1456318239000 (ms)
    joined_duration       HTML "Đã tham gia: 8 năm 5 tháng"   "8 năm 5 tháng" (nguyên văn, tính tới lúc tải trang)
    profile_updated_at    update_time (s)               1589256726000 (ms)
    is_active             is_active                     true
    phone_verified        phone_verified ("t"/"f")      true
    email_verified        email_verified ("t"/"f")      false

(*) name, avatar: ưu tiên trang người bán, không có thì giữ giá trị từ trang tin.
banner: ảnh bìa người bán, nhiều tài khoản không có (null). Không nhầm với iconBanners.banner (banner "tải app" chung).

Không lấy id_number, id_place (dữ liệu cá nhân nhạy cảm), phone/old_phone (luôn rỗng).
rating / follow trong initialState là state chưa tải (luôn 0) nên không dùng.

- Mỗi account_oid chỉ tải 1 lần: kết quả cache trong bộ nhớ (lượt chạy hiện tại) và Redis
  seller:<account_oid> (SELLER_PROFILE_CACHE_TTL_HOURS).
- Tải lỗi, trang không có dữ liệu: các field để null, tin vẫn được gửi. Không cache kết quả lỗi.
"""

import html
import json
import logging
import re

from chotot.parsers.base import SELLER_URL_PREFIX
from chotot.utils.page_cache import PageCache

PROFILE_PATHS = (
    ("props", "pageProps", "profile"),
    ("props", "initialProps", "pageProps", "profile"),
)
FIELDS = ("banner", "address", "about", "gender", "joined_at", "joined_duration", "profile_updated_at",
          "is_active", "phone_verified", "email_verified")
# Ưu tiên lấy từ trang người bán; không có thì giữ giá trị lấy từ trang tin (parser)
PREFERRED_FIELDS = ("name", "avatar")
# Mọi key parse_profile() trả về. Cache thiếu key nào (ghi bởi code cũ, trước khi thêm field) thì coi
# như chưa có và tải lại trang, không cần xóa cache bằng tay mỗi khi thêm field mới
CACHE_KEYS = frozenset(PREFERRED_FIELDS + FIELDS)

logger = logging.getLogger(__name__)


# HTML trang người bán: <p>Đã tham gia: <!-- --> 8 năm 5 tháng </p>. Đọc theo chữ nhãn, không theo class CSS
JOINED_RE = re.compile(r"Đã tham gia\s*:\s*((?:[^<]|<!--.*?-->)*)<", re.S)


def parse_joined_duration(page):
    """'Đã tham gia: <!-- --> 8 năm 5 tháng' → '8 năm 5 tháng' (nguyên văn, web tự tính tới lúc tải trang)."""
    match = JOINED_RE.search(page or "")
    if not match:
        return None
    text = re.sub(r"<!--.*?-->", "", match.group(1), flags=re.S)
    text = re.sub(r"\s+", " ", html.unescape(text)).strip()
    return text or None


def cache_key(account_oid):
    return f"seller:{account_oid}"


def _dig(data, path):
    for key in path:
        if not isinstance(data, dict):
            return None
        data = data.get(key)
    return data


def _sec_to_ms(value):
    """Timestamp giây (int/chuỗi) → mili giây. Giá trị đã là ms (>= 1e12) giữ nguyên."""
    try:
        value = int(value)
    except (TypeError, ValueError):
        return None
    if value <= 0:
        return None
    return value if value >= 10**12 else value * 1000


def _flag(value):
    """'t'/'f', True/False, 1/0 → bool; rỗng/không rõ → None."""
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower() if value is not None else ""
    if text in ("t", "true", "1", "yes"):
        return True
    if text in ("f", "false", "0", "no"):
        return False
    return None


def _text(value):
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value or None


def _find_profile(data, account_oid, depth=0):
    """Tìm dict có account_oid trùng khớp ở bất kỳ đâu trong JSON (phòng khi trang bị chuyển hướng
    để profile ở chỗ khác props.pageProps.profile)."""
    if depth > 12:
        return None
    if isinstance(data, dict):
        if data.get("account_oid") == account_oid and ("full_name" in data or "create_time" in data):
            return data
        children = data.values()
    elif isinstance(data, list):
        children = data
    else:
        return None
    for child in children:
        found = _find_profile(child, account_oid, depth + 1)
        if found is not None:
            return found
    return None


def parse_profile(next_data, account_oid=None):
    """next_data: dict JSON __NEXT_DATA__ của trang người bán. Trả về dict FIELDS hoặc None."""
    profile = None
    for path in PROFILE_PATHS:
        profile = _dig(next_data, path)
        if isinstance(profile, dict) and profile:
            break
    if (not isinstance(profile, dict) or not profile) and account_oid:
        profile = _find_profile(next_data, account_oid)
    if not isinstance(profile, dict) or not profile:
        return None
    return {
        "name": _text(profile.get("full_name")),
        "avatar": _text(profile.get("avatar")),
        "banner": _text(profile.get("banner")),
        "address": _text(profile.get("address")),
        "about": _text(profile.get("description")),   # giới thiệu người bán
        "gender": _text(profile.get("gender")),        # thường rỗng
        "joined_at": _sec_to_ms(profile.get("create_time")) or _sec_to_ms(profile.get("start_time")),
        "profile_updated_at": _sec_to_ms(profile.get("update_time")),
        "is_active": _flag(profile.get("is_active")),
        "phone_verified": _flag(profile.get("phone_verified")),
        "email_verified": _flag(profile.get("email_verified")),
    }


def apply(seller, info):
    """Ghi các field trang người bán vào item["seller"] (None nếu không có)."""
    info = info or {}
    for field in FIELDS:
        seller[field] = info.get(field)
    for field in PREFERRED_FIELDS:
        if info.get(field):
            seller[field] = info[field]


class SellerProfile(PageCache):
    name = "seller_profile"
    label = "Trang người bán"
    cache_prefix = "seller"
    meta_key = "seller_oid"
    required_keys = CACHE_KEYS
    enabled_setting = "SELLER_PROFILE_ENABLED"
    ttl_setting = "SELLER_PROFILE_CACHE_TTL_HOURS"
    default_ttl_hours = 72
    failure_fields = FIELDS  # name, avatar không tính: lỗi thì vẫn có giá trị từ trang tin

    def build_request(self, account_oid, callback, errback, meta=None):
        # Tài khoản có alias: /user/<oid> → /user/<alias> (vd /user/HDMobileHP01), Scrapy tự theo redirect
        return super().build_request(account_oid, f"{SELLER_URL_PREFIX}{account_oid}", callback, errback, meta)

    def parse(self, response, account_oid):
        raw = response.css("script#__NEXT_DATA__::text").get()
        try:
            info = parse_profile(json.loads(raw), account_oid) if raw else None
        except ValueError:
            info = None
        if info is not None:
            # "Đã tham gia: 8 năm 5 tháng" trong HTML (không có trong __NEXT_DATA__)
            info["joined_duration"] = parse_joined_duration(response.text)
        return info
