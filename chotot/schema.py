"""Schema cố định của các object lồng trong item (gửi Kafka).

Mọi tin, ở mọi site (chotot, xe, vieclamtot, nhatot), đều có ĐỦ các key dưới đây, đúng thứ tự này.
Không có giá trị thì null. Phía sau (Kafka consumer, ES mapping...) luôn đọc được key, không phải kiểm tra
key có tồn tại hay không.

Thêm field mới: thêm tên vào tuple tương ứng ở đây (và điền giá trị ở parser / spider).
Key không có trong schema vẫn được giữ (đặt cuối) nhưng có log cảnh báo để bổ sung vào đây.

specs / specs_labels KHÔNG theo schema cố định: đó là map "mã thông số → giá trị", mỗi loại tin một bộ
(xe có mileage, nhà có rooms...). Phía sau xử lý như map.
"""

import logging

logger = logging.getLogger(__name__)

SELLER_FIELDS = (
    # Định danh, từ trang tin
    "account_id",
    "account_oid",
    "url",                       # https://www.chotot.com/user/<account_oid>
    "name",                      # ưu tiên trang người bán, không có thì trang tin
    "avatar",                    # ưu tiên trang người bán, không có thì trang tin
    "is_company",
    "phone",                     # API phone
    # Trang người bán (utils/seller_profile.py)
    "banner",
    "address",
    "about",                     # giới thiệu (profile.description)
    "gender",                    # profile.gender, thường rỗng
    "joined_at",
    "joined_duration",           # "Đã tham gia: 8 năm 5 tháng" trên trang người bán (nguyên văn)
    "profile_updated_at",
    "is_active",
    "phone_verified",
    "email_verified",
    # Số liệu người bán: API public (utils/seller_stats.py), dự phòng trang tin
    "followers",
    "followings",
    "rating",
    "total_rating",              # tổng mọi đánh giá (vai trò người bán + người mua)
    "total_rating_as_seller",    # đánh giá nhận khi là người bán (API ratings)
    "total_rating_as_buyer",     # đánh giá nhận khi là người mua (API ratings)
    "sold_ads",
    "live_ads",
    "response_rate",             # tỷ lệ phản hồi chat, % (API chat)
    "response_rate_text",        # vd "Thỉnh thoảng"
    "response_time",             # nguyên giá trị API (vd -1)
    "response_time_text",        # vd "Phản hồi chậm"
    # Nhà tuyển dụng (vieclamtot)
    "company_name",
    "company_logo",
)

LOCATION_FIELDS = (
    "address",
    "city",
    "city_v3",                   # tên tỉnh/thành sau sáp nhập
    "district",
    "ward",
    "ward_v3",                   # tên phường/xã sau sáp nhập
    "street",                    # nhatot
    "region_id",
    "area_id",
    "ward_id",
    "lat",
    "lng",
)

# field của AdItem → schema object lồng
NESTED_SCHEMAS = {
    "seller": SELLER_FIELDS,
    "location": LOCATION_FIELDS,
}

_warned = set()


def conform(value, fields, name=""):
    """dict bất kỳ → dict đủ `fields` theo đúng thứ tự (thiếu thì None). Key lạ giữ lại ở cuối."""
    value = value if isinstance(value, dict) else {}
    result = {field: value.get(field) for field in fields}
    for key, val in value.items():
        if key not in result:
            if (name, key) not in _warned:
                _warned.add((name, key))
                logger.warning("Field %s.%s chưa có trong schema (chotot/schema.py), cần bổ sung", name, key)
            result[key] = val
    return result


def conform_item(adapter):
    """Áp schema cho các object lồng của 1 AdItem (ItemAdapter hoặc dict)."""
    for field, fields in NESTED_SCHEMAS.items():
        adapter[field] = conform(adapter.get(field), fields, field)

