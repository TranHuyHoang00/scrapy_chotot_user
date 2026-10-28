"""Số liệu người bán qua API public của Chợ Tốt (không cần token), gọi lần lượt 6 API theo account_oid:

    API                                                                          → seller
    GET /v1/public/social/followers/count/<oid>           {"ok":true,"result":55}     followers
    GET /v1/public/social/followings/countall/<oid>       {"ok":true,"result":11}     followings
    GET /v1/public/solds_by/<oid>?limit=1&offset=0        {"paging":{"total":1180}}   sold_ads
    GET /v1/public/theia/<oid>?limit=1&page=1             {"total":22, ...}           live_ads
    GET /v1/public/ratings/<oid>?post_type=all&page=1&limit=1&version=2.0.0&rejected=1
                                                          {"average_rating":5,        rating
                                                           "total_rating":23,         total_rating (tổng mọi đánh giá)
                                                           "total_review_as_seller":21, total_rating_as_seller
                                                           "total_review_as_buyer":2}   total_rating_as_buyer
    GET /v2/public/chat/user/get/<oid>                    {"result":{                 (Phản hồi chat)
                                                           "response_rate":0.93,      response_rate = 93 (%, làm
                                                                                       tròn xuống như web; -1 → null)
                                                           "response_rate_text":      response_rate_text
                                                             "Thỉnh thoảng",
                                                           "response_time":-1,        response_time (nguyên giá trị API)
                                                           "response_time_text":      response_time_text
                                                             "Phản hồi chậm"}}

Dùng chung cho mọi site (chotot, xe, vieclamtot, nhatot).

- sold_ads, live_ads, rating, total_rating: ưu tiên giá trị API; API lỗi / không có thì giữ giá trị lấy từ
  trang tin (parser). Các field còn lại (followers, response_rate...): chỉ có từ API, lỗi thì null.
- Mỗi API lỗi riêng (HTTP lỗi, timeout, JSON sai) chỉ làm field của API đó = null, vẫn gọi API tiếp theo.
- Mỗi người bán gọi 1 lượt (1 request/API), cache bộ nhớ + Redis seller_stats:<account_oid>
  (SELLER_STATS_CACHE_TTL_HOURS). Không cache nếu mọi API đều lỗi.
- Tốc độ riêng (download slot "seller-stats"): SELLER_STATS_DELAY, SELLER_STATS_CONCURRENCY, tách khỏi
  API phone dù cùng domain gateway.chotot.com.
- Thêm / đổi API: sửa APIS bên dưới.
"""

import json
import logging

import scrapy

from chotot.utils.page_cache import PageCache

API_BASE = "https://gateway.chotot.com/v1/public"
API_BASE_V2 = "https://gateway.chotot.com/v2/public"
HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.chotot.com",
    "Referer": "https://www.chotot.com/",
}
DOWNLOAD_SLOT = "seller-stats"


def _dig(data, *path):
    for key in path:
        if not isinstance(data, dict):
            return None
        data = data.get(key)
    return data


def _number(value):
    """int/float hoặc None (bỏ bool, chuỗi số thì đổi)."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value) if float(value).is_integer() else value
    try:
        number = float(str(value).replace(",", "."))
    except ValueError:
        return None
    return int(number) if number.is_integer() else number


def _percent(rate):
    """response_rate 0.93 → 93 (làm tròn xuống như web: Math.floor(100 * rate)). -1 / không có → None."""
    rate = _number(rate)
    if rate is None or rate < 0:
        return None
    return int(rate * 100 + 1e-9)


def _text(value):
    """Chuỗi đã bỏ khoảng trắng; rỗng / không phải chuỗi → None."""
    if not isinstance(value, str):
        return None
    return value.strip() or None


# (tên API dùng trong log/thống kê, URL theo oid, hàm JSON → {field seller: giá trị})
APIS = (
    ("followers",
     lambda oid: f"{API_BASE}/social/followers/count/{oid}",
     lambda data: {"followers": _number(_dig(data, "result"))}),
    ("followings",
     lambda oid: f"{API_BASE}/social/followings/countall/{oid}",
     lambda data: {"followings": _number(_dig(data, "result"))}),
    ("solds",
     lambda oid: f"{API_BASE}/solds_by/{oid}?limit=1&offset=0",
     lambda data: {"sold_ads": _number(_dig(data, "paging", "total"))}),
    ("theia",
     lambda oid: f"{API_BASE}/theia/{oid}?limit=1&page=1",
     lambda data: {"live_ads": _number(_dig(data, "total") if _dig(data, "total") is not None
                                       else _dig(data, "paging", "total"))}),
    ("ratings",
     lambda oid: f"{API_BASE}/ratings/{oid}?post_type=all&page=1&limit=1&version=2.0.0&rejected=1",
     lambda data: {"rating": _number(_dig(data, "average_rating")),
                   "total_rating": _number(_dig(data, "total_rating")),
                   "total_rating_as_seller": _number(_dig(data, "total_review_as_seller")),
                   "total_rating_as_buyer": _number(_dig(data, "total_review_as_buyer"))}),
    ("chat",
     lambda oid: f"{API_BASE_V2}/chat/user/get/{oid}",
     lambda data: {"response_rate": _percent(_dig(data, "result", "response_rate")),
                   "response_rate_text": _text(_dig(data, "result", "response_rate_text")),
                   "response_time": _number(_dig(data, "result", "response_time")),
                   "response_time_text": _text(_dig(data, "result", "response_time_text"))}),
)

FIELDS = ("followers", "followings", "sold_ads", "live_ads", "rating", "total_rating",
          "total_rating_as_seller", "total_rating_as_buyer",
          "response_rate", "response_rate_text", "response_time", "response_time_text")
# API là nguồn chính, nhưng trang tin cũng có: API không có giá trị thì giữ giá trị trang tin
FALLBACK_FIELDS = frozenset(("sold_ads", "live_ads", "rating", "total_rating"))

logger = logging.getLogger(__name__)


def apply(seller, info):
    """Ghi số liệu API vào seller. info None (lỗi hết) thì followers/followings = null, phần còn lại giữ nguyên."""
    info = info or {}
    for field in FIELDS:
        value = info.get(field)
        if value is not None or field not in FALLBACK_FIELDS:
            seller[field] = value


class SellerStats(PageCache):
    name = "seller_stats"
    label = "API số liệu người bán"
    cache_prefix = "seller_stats"
    meta_key = "stats_oid"
    required_keys = frozenset(FIELDS)
    enabled_setting = "SELLER_STATS_ENABLED"
    ttl_setting = "SELLER_STATS_CACHE_TTL_HOURS"
    default_ttl_hours = 24  # theo dõi, đã bán, đánh giá đổi thường xuyên
    failure_fields = FIELDS

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._callbacks = (None, None)  # (callback, errback) của spider, dùng cho các API nối tiếp
        self._base_meta = {}            # account_oid → meta gốc (profile, proxy...) cho các API nối tiếp

    def _request(self, account_oid, index, info, ok, unknown, callback, errback, meta):
        api_name, make_url, _extract = APIS[index]
        meta = dict(meta or {})
        meta.update({
            self.meta_key: account_oid,
            "stats_index": index,
            "stats_info": info,
            "stats_ok": ok,
            "stats_unknown": unknown,
            "download_slot": DOWNLOAD_SLOT,
            "handle_httpstatus_all": True,  # tự xử lý mã lỗi để đi tiếp API sau
        })
        self._inc(f"request/{api_name}")
        return scrapy.Request(
            make_url(account_oid),
            headers=HEADERS,
            callback=callback,
            errback=errback,
            meta=meta,
            dont_filter=True,
        )

    def build_request(self, account_oid, callback, errback, meta=None):
        """Request API đầu tiên. Các API sau được nối tiếp trong next_step()."""
        self._inc("request")
        self._callbacks = (callback, errback)
        self._base_meta[account_oid] = dict(meta or {})
        return self._request(account_oid, 0, {}, 0, [], callback, errback, self._base_meta[account_oid])

    def _extract(self, response):
        """(dict field → giá trị, lỗi?) của 1 response. Lỗi (HTTP, JSON sai) → ({}, True);
        API trả 200 nhưng không có số liệu (vd response_rate -1) → ({}, False): null thật, không phải lỗi."""
        api_name, _make_url, extract = APIS[response.meta["stats_index"]]
        oid = response.meta.get(self.meta_key)
        if response.status != 200:
            self._inc(f"error/{api_name}")
            logger.warning("%s %s: API %s trả về HTTP %s", self.label, oid, api_name, response.status)
            return {}, True
        try:
            data = json.loads(response.text)
        except ValueError:
            self._inc(f"error/{api_name}")
            logger.warning("%s %s: API %s trả về không phải JSON", self.label, oid, api_name)
            return {}, True
        values = extract(data)
        if all(v is None for v in values.values()):
            self._inc(f"no_data/{api_name}")
            return {}, False
        self._inc(f"ok/{api_name}")
        return values, False

    def next_step(self, meta, values, failed):
        """Gộp kết quả 1 API, trả về Request API kế tiếp hoặc (account_oid, info) khi xong mọi API.
        info["_unknown"]: field của các API bị lỗi (null do lỗi)."""
        oid = meta[self.meta_key]
        index = meta["stats_index"]
        info = {**meta["stats_info"], **values}
        ok = meta["stats_ok"] + (0 if failed else 1)
        unknown = list(meta.get("stats_unknown") or [])
        if failed:
            unknown += [f for f in APIS[index][2]({}) if f not in unknown]  # field của API này
        if index + 1 < len(APIS):
            callback, errback = self._callbacks
            return self._request(oid, index + 1, info, ok, unknown, callback, errback, self._base_meta.get(oid))
        self._base_meta.pop(oid, None)
        info = {field: info.get(field) for field in FIELDS}
        info["_unknown"] = unknown
        if ok:
            self._inc("ok")
            self.set_cached(oid, info)
        else:
            self._inc("error")
            logger.warning("%s %s: cả %d API đều lỗi, không cache", self.label, oid, len(APIS))
        return oid, info

    def on_response(self, response):
        values, failed = self._extract(response)
        return self.next_step(response.meta, values, failed=failed)

    def on_step_failure(self, failure):
        """Lỗi mạng / timeout 1 API (sau khi retry): field của API đó = null, đi tiếp API sau."""
        meta = failure.request.meta
        api_name = APIS[meta["stats_index"]][0]
        self._inc(f"error/{api_name}")
        logger.warning("%s %s: API %s lỗi: %s", self.label, meta.get(self.meta_key), api_name,
                       failure.getErrorMessage())
        return self.next_step(meta, {}, failed=True)
