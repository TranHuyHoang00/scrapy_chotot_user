import json

import scrapy
from scrapy.exceptions import CloseSpider
from w3lib.url import add_or_replace_parameter

from chotot.parsers import ALLOWED_DOMAINS, SITES, get_parser, site_of, site_setting
from chotot.spiders.base import HydraBaseSpider
from chotot.utils.hydra import get_ad
from chotot.utils import seller_profile, seller_stats
from chotot.utils.phone_api import PhoneApi, PhoneAuthError, PhoneTokenError
from chotot.utils.seller_profile import SellerProfile
from chotot.utils.seller_stats import SellerStats

# Lý do dừng spider khi không lấy được token API phone (lúc khởi động hoặc sau khi thử lại hết số lần)
PHONE_TOKEN_ERROR_REASON = "phone_token_error"

AD_LINK_REGEX = r"^[^#?]*/\d+\.htm"
LIST_PRIORITY = -10  # trang danh sách chạy sau mọi request của tin (xem list_request)

# Các bước bổ sung thông tin sau trang chi tiết tin (xem ChototSpider._advance)
STEP_SELLER, STEP_STATS, STEP_PHONE = 0, 1, 2
PHONE_CALL = object()  # _advance(): cần gọi API phone


class ChototSpider(HydraBaseSpider):
    """Crawl trang tổng hợp toàn bộ tin: https://www.chotot.com/mua-ban?page=N

    URL lấy từ seed_url/url của profile Hydra; profile không có URL hợp lệ thì dùng CHOTOT_START_URL.
    Link chi tiết có thể thuộc các site khác của Chợ Tốt (xe.chotot.com, vieclamtot.com, nhatot.com);
    mỗi site có parser riêng trong chotot/parsers/.

    Có thể khai báo thêm trong profile (không bắt buộc):
        max_pages            số trang danh sách tối đa (mặc định CHOTOT_MAX_PAGES)
        max_items_per_page   số tin tối đa mỗi trang, 0 = lấy hết
    """

    name = "chotot"
    allowed_domains = ALLOWED_DOMAINS

    def setup_hydra(self):
        super().setup_hydra()
        # None nếu PHONE_API_ENABLED=false. API token đi qua cùng proxy với crawl
        self.phone_api = PhoneApi.from_crawler(
            self.crawler,
            proxy=self.my_proxy,
            log=lambda level, message: self.monitor(level, message, tag="PHONE"),
        )
        self.phone_auth_failed = False
        # None nếu SELLER_PROFILE_ENABLED=false / SELLER_STATS_ENABLED=false
        self.seller_profile = SellerProfile.from_crawler(self.crawler)
        self.seller_stats = SellerStats.from_crawler(self.crawler)
        # {tên trang: {key: [(envelope, profile)]}}: tin đang chờ 1 trang đang được tải
        self.pending = {}
        # Site được crawl (CRAWL_CHOTOT / CRAWL_XE / CRAWL_VIECLAMTOT / CRAWL_NHATOT)
        self.enabled_sites = {site for site in SITES if self.settings.getbool(site_setting(site), True)}

    def start_requests(self):
        """Lấy token API phone trước khi đọc profile: lỗi thì dừng ngay, không lấy profile khỏi Kafka."""
        if self.phone_api is not None:
            try:
                self.phone_api.init_token()
            except PhoneTokenError as e:
                self.phone_auth_failed = True
                self.monitor("ERROR", f"Không lấy được token API phone: {e}. Dừng spider", tag="PHONE")
                raise CloseSpider(PHONE_TOKEN_ERROR_REASON)
        yield from super().start_requests()

    def _inc(self, name):
        self.crawler.stats.inc_value(f"chotot/{name}")

    # ------------------------------------------------------------ danh sách

    def site_allowed(self, url):
        """False nếu link tin thuộc site đã tắt (CRAWL_<SITE>=false). Domain chưa có parser vẫn cho qua
        (parse_detail báo 'Chưa có parser')."""
        site = site_of(url)
        if site is None or site in self.enabled_sites:
            return True
        self._inc(f"skipped_site/{site}")
        return False

    def start_profile(self, profile, url):
        if self.detail_urls:  # test tin cụ thể: vào thẳng trang chi tiết, bỏ trang danh sách
            for detail_url in self.detail_urls:
                if not self.site_allowed(detail_url):
                    self.monitor("WARNING", f"[DETAIL-TEST] Bỏ qua tin {detail_url}: "
                                            f"{site_setting(site_of(detail_url))}=false")
                    continue
                yield scrapy.Request(
                    detail_url,
                    callback=self.parse_detail,
                    errback=self.detail_failed,
                    meta=self.request_meta(profile),
                    dont_filter=True,
                )
            return
        if "chotot.com" not in url:
            url = self.settings.get("CHOTOT_START_URL")
        yield self.list_request(profile, url, 1)

    def list_request(self, profile, base_url, page):
        page_param = self.settings.get("CHOTOT_PAGE_PARAM", "page")
        url = base_url if page == 1 else add_or_replace_parameter(base_url, page_param, str(page))
        return scrapy.Request(
            url,
            callback=self.parse,
            # chỉ trang 1 lỗi mới đánh dấu profile dead
            errback=self.error_back_start_request if page == 1 else None,
            meta=self.request_meta(profile, base_url=base_url, page=page),
            dont_filter=True,
            # Ưu tiên thấp hơn mọi request của tin (trang tin, trang người bán, API số liệu, phone: priority 0):
            # crawl xong tin của trang hiện tại rồi mới sang trang sau, item lên Kafka đều từ trang 1.
            # Không đặt thì hàng đợi LIFO của Scrapy chạy trang sau trước, tin bị dồn tới cuối.
            priority=LIST_PRIORITY,
        )

    def _profile_int(self, profile, key, setting, default):
        value = profile.get(key)
        if value in (None, ""):
            return self.settings.getint(setting, default)
        return int(value)

    def extract_links(self, response):
        """Link tin nằm trong <li> của danh sách: <li ...><div ...><a href=".../135011939.htm">.
        Không dùng class (a14axl8t, cwv3xk0...) vì là class sinh tự động, dễ đổi."""
        hrefs = response.css("li a::attr(href)").re(AD_LINK_REGEX)
        links = [response.urljoin(h) for h in hrefs]
        return list(dict.fromkeys(links))

    def parse(self, response):
        profile = response.meta["profile"]
        page = response.meta["page"]
        profile_id = profile.get("profile_id")

        all_links = self.extract_links(response)
        if not all_links:
            self._inc("list_empty")
            self.monitor("WARNING", f"[{profile_id}] Trang {page} không có link tin, dừng phân trang: {response.url}")
            return

        self._inc("list_pages")
        if page == 1:
            self.update_status_source(profile_id, ["crawled", "alive"], once=True)

        # Bỏ tin của site đã tắt trước khi tải (không tốn request); trang chỉ toàn tin site tắt vẫn sang trang sau
        allowed = [link for link in all_links if self.site_allowed(link)]
        skipped = len(all_links) - len(allowed)
        limit = self._profile_int(profile, "max_items_per_page", "CHOTOT_MAX_ITEMS_PER_PAGE", 0)
        links = allowed[:limit] if limit else allowed
        self.logger.info("[%s] Trang %s: %d link tin, crawl %d%s", profile_id, page, len(all_links), len(links),
                         f", bỏ {skipped} tin của site đã tắt" if skipped else "")

        for link in links:
            yield scrapy.Request(
                link,
                callback=self.parse_detail,
                meta=self.request_meta(profile),
            )

        max_pages = self._profile_int(profile, "max_pages", "CHOTOT_MAX_PAGES", 5)
        if page < max_pages:
            yield self.list_request(profile, response.meta["base_url"], page + 1)

    # ------------------------------------------------------------- chi tiết

    def detail_failed(self, failure):
        """Chỉ dùng ở luồng test detail_url: báo lỗi tải trang tin."""
        status = getattr(getattr(failure.value, "response", None), "status", None)
        self.monitor(
            "ERROR",
            f"[DETAIL-TEST] Không tải được tin {failure.request.url}: "
            f"{f'HTTP {status}' if status else failure.getErrorMessage()}",
        )

    async def parse_detail(self, response):
        profile = response.meta["profile"]

        parser = get_parser(response.url)
        if parser is None:
            self._inc("detail_unknown_site")
            self.monitor("WARNING", f"Chưa có parser cho domain: {response.url}")
            return

        raw = response.css("script#__NEXT_DATA__::text").get()
        if not raw:
            self._inc(f"{parser.site}/no_json")
            self.monitor("WARNING", f"[{parser.site}] Không có __NEXT_DATA__: {response.url}")
            return

        try:
            next_data = json.loads(raw)
        except ValueError:
            self._inc(f"{parser.site}/bad_json")
            return

        item = parser.parse(next_data, response.url)
        if item is None:
            self._inc(f"{parser.site}/no_ad")
            self.monitor("WARNING", f"[{parser.site}] Không có dữ liệu tin: {response.url}")
            return

        self._inc(f"{parser.site}/parsed")
        envelope = self.build_item_data(profile, item["url"], item["ad_id"], item)

        # Thứ tự: trang người bán → API số liệu người bán → API phone → pipeline
        async for result in self.advance(envelope, profile, STEP_SELLER):
            yield result

    # ------------------------------------------------- bổ sung thông tin người bán
    #
    # Sau trang chi tiết tin, mỗi tin đi lần lượt qua các bước (bước nào tắt / không áp dụng thì bỏ qua):
    #   STEP_SELLER  trang người bán /user/<account_oid>   (utils/seller_profile.py)
    #   STEP_STATS   API số liệu người bán (theo dõi, đã bán, đang đăng, đánh giá, phản hồi chat)  (utils/seller_stats.py)
    #   STEP_PHONE   API số điện thoại                      (utils/phone_api.py)
    # rồi mới vào pipeline. Trang người bán: lấy từ cache, hoặc tải trang (nhiều tin cùng người bán
    # chỉ tải 1 lần, tin đến sau chờ trong self.pending).

    def _enrich_pages(self):
        """(step, PageCache, hàm lấy key từ ad, hàm lấy url, hàm điền info vào seller, callback, errback).
        Hàm lấy key trả None = bỏ qua bước này cho tin đó."""
        return (
            (STEP_SELLER, self.seller_profile,
             lambda ad: ad["seller"].get("account_oid"),
             lambda ad, key: None,  # SellerProfile tự dựng URL từ account_oid
             seller_profile.apply, self.parse_seller, self.seller_failed),
            (STEP_STATS, self.seller_stats,
             lambda ad: ad["seller"].get("account_oid"),
             lambda ad, key: None,  # SellerStats tự dựng URL các API từ account_oid
             seller_stats.apply, self.parse_stats, self.stats_failed),
        )

    def _advance(self, envelope, profile, step):
        """Chạy các bước từ `step`. Trả về Request cần tải, envelope (xong, gửi đi),
        PHONE_CALL (cần gọi API phone), hoặc None (đang chờ trang khác / bỏ tin)."""
        ad = get_ad(envelope)
        seller = ad["seller"]
        for page_step, page, get_key, get_url, apply_info, callback, errback in self._enrich_pages():
            if step > page_step or page is None:
                continue
            key = get_key(ad)
            if not key:
                continue
            info = page.get_cached(key)
            if info is not None:
                apply_info(seller, info)
                self._note_unknown(envelope, page.unknown_fields(info))
                continue
            pending = self.pending.setdefault(page.name, {})
            if key in pending:  # đã có request đang tải trang này: chờ chung
                pending[key].append((envelope, profile))
                return None
            pending[key] = [(envelope, profile)]
            url = get_url(ad, key)
            meta = self.request_meta(profile)
            if url is None:
                return page.build_request(key, callback=callback, errback=errback, meta=meta)
            return page.build_request(key, url, callback=callback, errback=errback, meta=meta)

        action = self._phone_action(envelope)
        if action == "send":
            return envelope
        if action == "drop":
            return None
        return PHONE_CALL

    async def advance(self, envelope, profile, step=STEP_SELLER):
        result = self._advance(envelope, profile, step)
        if result is PHONE_CALL:
            # Token quá exp: tạm dừng spider, lấy token mới trước khi gọi API phone
            if self.phone_api.token_expired() and not await self._renew_phone_token():
                return
            result = self.phone_request(envelope, profile)
        if result is not None:
            yield result

    def advance_now(self, envelope, profile, step):
        """Như advance() nhưng không chờ lấy token (dùng trong errback, không async): token hết hạn
        thì API phone trả 401, parse_phone sẽ lấy token mới rồi gọi lại."""
        result = self._advance(envelope, profile, step)
        if result is PHONE_CALL:
            result = self.phone_request(envelope, profile)
        return result

    def _release(self, page, key, info, apply_info):
        """Lấy các tin đang chờ trang `key`, điền info (None = để null)."""
        waiting = self.pending.get(page.name, {}).pop(key, [])
        unknown = page.unknown_fields(info)
        for envelope, _profile in waiting:
            apply_info(get_ad(envelope)["seller"], info)
            self._note_unknown(envelope, unknown)
        return waiting

    @staticmethod
    def _note_unknown(envelope, fields):
        """Ghi lại field seller bị null DO LỖI (trang / API lỗi). Chống trùng dùng để giữ giá trị đã gửi lần
        trước thay vì ghi đè bằng null (pipelines/redis_dedup.py)."""
        if not fields:
            return
        unknown = envelope["item_data"].setdefault("unknown", [])
        unknown.extend(f for f in fields if f not in unknown)

    # trang người bán

    async def parse_seller(self, response):
        info = self.seller_profile.parse_response(response)
        key = response.meta[self.seller_profile.meta_key]
        for envelope, profile in self._release(self.seller_profile, key, info, seller_profile.apply):
            async for result in self.advance(envelope, profile, STEP_STATS):
                yield result

    def seller_failed(self, failure):
        """Lỗi tải trang người bán: vẫn đi tiếp với các field người bán = null."""
        self.seller_profile.on_failure(failure)
        key = failure.request.meta[self.seller_profile.meta_key]
        for envelope, profile in self._release(self.seller_profile, key, None, seller_profile.apply):
            result = self.advance_now(envelope, profile, STEP_STATS)
            if result is not None:
                yield result

    # API số liệu người bán: mỗi response gọi API tiếp theo, xong hết thì đi tiếp các tin đang chờ

    async def parse_stats(self, response):
        result = self.seller_stats.on_response(response)
        if isinstance(result, scrapy.Request):
            yield result
            return
        key, info = result
        for envelope, profile in self._release(self.seller_stats, key, info, seller_stats.apply):
            async for item in self.advance(envelope, profile, STEP_PHONE):
                yield item

    def stats_failed(self, failure):
        """Lỗi mạng/timeout 1 API: field của API đó = null, vẫn gọi API tiếp theo."""
        result = self.seller_stats.on_step_failure(failure)
        if isinstance(result, scrapy.Request):
            yield result
            return
        key, info = result
        for envelope, profile in self._release(self.seller_stats, key, info, seller_stats.apply):
            item = self.advance_now(envelope, profile, STEP_PHONE)
            if item is not None:
                yield item

    # ----------------------------------------------------------- số điện thoại

    def _phone_action(self, envelope):
        """Bước API phone cần làm: "send" (gửi tin luôn), "call" (gọi API phone), "drop" (bỏ tin)."""
        ad = get_ad(envelope)
        if self.phone_api is None or not ad.get("ad_id"):
            return "send"
        if self.phone_auth_failed:  # token đã lỗi, spider đang dừng: bỏ tin để lần sau crawl lại đủ số
            return "drop"
        phone = self.phone_api.get_cached(ad["site"], ad["ad_id"])
        if phone:
            ad["seller"]["phone"] = phone
            return "send"
        return "call"

    def phone_request(self, envelope, profile):
        return self.phone_api.build_request(
            get_ad(envelope)["ad_id"],
            callback=self.parse_phone,
            errback=self.phone_failed,
            meta=self.request_meta(profile, envelope=envelope),
        )

    async def _renew_phone_token(self, failed_generation=None):
        """Lấy token mới (spider tạm dừng trong lúc chờ). True = có token, gọi lại API phone.
        False = spider đang đóng nên bỏ qua. Hết số lần thử thì dừng spider (CloseSpider).
        Tin đang chờ số không được gửi: chưa đánh dấu chống trùng nên lần chạy sau sẽ crawl lại đủ số."""
        if self.phone_auth_failed:
            raise CloseSpider(PHONE_TOKEN_ERROR_REASON)
        if await self.phone_api.renew_token(failed_generation):
            return True
        if self.phone_api.renew_aborted():
            return False
        self.phone_auth_failed = True
        raise CloseSpider(PHONE_TOKEN_ERROR_REASON)

    async def parse_phone(self, response):
        envelope = response.meta["envelope"]
        ad = get_ad(envelope)
        try:
            ad["seller"]["phone"] = self.phone_api.parse_response(response, ad["site"])
            if self.phone_api.is_error_response(response):
                self._note_unknown(envelope, ["phone"])
        except PhoneAuthError as e:
            self.logger.warning("API phone %s:%s bị từ chối (%s)", ad["site"], ad["ad_id"], e)
            if await self._renew_phone_token(e.generation):
                # Đã có token mới: gọi lại API phone cho tin này
                yield self.phone_request(envelope, response.meta["profile"])
            return
        yield envelope

    def phone_failed(self, failure):
        """Lỗi mạng/timeout khi gọi API phone: vẫn gửi tin với phone = null."""
        envelope = failure.request.meta["envelope"]
        if self.phone_auth_failed:
            return
        self.phone_api.on_failure(failure, get_ad(envelope)["site"])
        self._note_unknown(envelope, ["phone"])
        yield envelope
