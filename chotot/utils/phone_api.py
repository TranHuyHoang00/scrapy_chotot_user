"""Lấy số điện thoại đầy đủ của tin qua API Chợ Tốt.

    GET PHONE_API_URL?e=<make_e(ad_id)>   (ad_id = list_id, ID trên URL tin: .../135193883.htm;
                                          e = base64(RSA PKCS#1 v1.5(ad_id, PROD_PUBKEY)))
    Header: Authorization: Bearer <token>
    Response: {"phone": "0523149999"}

Token không đặt trong .env mà lấy từ API token (TokenProvider):

    GET PHONE_TOKEN_API_URL
    Header: X-Session-Keeper-Key: PHONE_TOKEN_API_KEY
    Response: {"token": "...", "cookie": "privateToken", "exp": 1792727075, "at": 1792654459950}
              exp: hết hạn (giây), at: lúc cấp (mili giây)

Dùng chung cho mọi site (chotot, xe, nhatot, vieclamtot).
Request API phone đi qua Scrapy (bất đồng bộ) nên tự theo proxy USE_PROXY / CRAWL_PROXY;
API token cũng đi qua proxy đó.

Mọi thứ liên quan đến API nằm trong file này: đổi API thì chỉ sửa ở đây.

Kết quả:
    - Có số: trả về chuỗi số, lưu cache Redis (PHONE_CACHE_TTL_HOURS).
    - Không có số / người bán ẩn số (403 "phone hidden") / lỗi mạng / lỗi HTTP khác: trả về None,
      tin vẫn được gửi với phone = null.
    - Token hết hạn (quá exp, hoặc API phone trả 401/403): tạm dừng spider, đợi PHONE_TOKEN_RETRY_DELAY
      giây rồi lấy token mới, thử tối đa PHONE_TOKEN_MAX_RETRIES lần. Hết số lần → dừng spider.
      Spider đang đóng thì bỏ qua, không lấy token nữa.
"""

import asyncio
import base64
import json
import logging
import time

import requests
import scrapy
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization import load_pem_public_key
from w3lib.url import add_or_replace_parameter

from chotot.utils.redis_client import get_redis

AUTH_ERROR_CODES = (401, 403)
# 403 kèm nội dung này là người bán ẩn số, không phải lỗi token
PHONE_HIDDEN_MARKERS = ("phone hidden",)
DEFAULT_URL = "https://gateway.chotot.com/v1/private/ad-listing/phone"
DEFAULT_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.chotot.com",
    "Referer": "https://www.chotot.com/",
}
TOKEN_KEY_HEADER = "X-Session-Keeper-Key"

PROD_PUBKEY = b"""-----BEGIN PUBLIC KEY-----
MIIBojANBgkqhkiG9w0BAQEFAAOCAY8AMIIBigKCAYEAxnvPjlA/K/adq6mA6+uU
tlyBBxFaKeK+WD2FypOeCAP0qtucmaDrIbxirykrxQjRpGxl2HKRBwGd2h/hDuk9
CxRUXD2p0Hrzb1Hb9M5px19TPXM6AWSClR1kozehRusIFrxP6PHqDLx5prJFLlSZ
zg3N3oGhS6oP/a4Ku/iAdCUCiHb5TX3b3+y4Ll/QViZhpKZjU6BhIOsiVIJhyXvn
0cSqLXPjNuXR5A4JkmRl9T9cWncEHTKmoVUyXQJaDZa3yH/OJSEmhhGyKNKkM5so
lasJWSBKenFnFvphw3+KG8BGfJwGkvtRAVbS1ljduH8z8fxALxHgUdnTtgpxB+KZ
/CVnNr97EGqYPLVlX+duGkuy1yCunqVTiY2HyL/0bMTBK84oCQjtMVAHgZ345hZn
mGST71D8+i5HGtOOFoRyP6qK6ex1qfEROzWsmVDA00aHLlQcKOLaHvT/DB30aeUs
ZoL/kQo100XccufpHESrits0mEuoyza4CCFM04F3pDOXAgMBAAE=
-----END PUBLIC KEY-----"""

_key = load_pem_public_key(PROD_PUBKEY)

logger = logging.getLogger(__name__)


class PhoneAuthError(Exception):
    """API phone trả 401/403: token hết hạn hoặc không hợp lệ."""

    def __init__(self, message, generation=None):
        super().__init__(message)
        self.generation = generation


class PhoneTokenError(Exception):
    """Không lấy được token hợp lệ từ API token."""


def make_e(ad_id: str) -> str:
    ct = _key.encrypt(str(ad_id).encode(), padding.PKCS1v15())
    return base64.b64encode(ct).decode()


def cache_key(site, ad_id):
    return f"phone:{site}:{ad_id}"


def clean_phone(value):
    if value is None:
        return None
    phone = str(value).strip().replace(" ", "").replace(".", "")
    return phone or None


def is_phone_hidden(text):
    text = (text or "").lower()
    return any(marker in text for marker in PHONE_HIDDEN_MARKERS)


def engine_closing(crawler):
    """Spider đang đóng (đủ item, Ctrl+C, CloseSpider...). Hỗ trợ Scrapy cũ (engine.slot) và mới (engine._slot)."""
    engine = getattr(crawler, "engine", None)
    slot = getattr(engine, "_slot", None) or getattr(engine, "slot", None)
    return getattr(slot, "closing", None) is not None


def _default_log(level, message):
    getattr(logger, level.lower())(message)


class TokenProvider:
    """Giữ token API phone, lấy lại khi hết hạn.

    generation tăng mỗi lần có token mới: request API phone ghi lại generation đã dùng,
    nhờ đó 401/403 của request dùng token cũ chỉ cần gửi lại, không phải lấy token lần nữa.

    failures đếm số lần thử lấy token kể từ lần gọi API phone thành công gần nhất
    (lấy token lỗi, hoặc token mới vẫn bị 401/403). Đạt max_retries → hỏng hẳn.
    """

    def __init__(self, url, key, proxy=None, timeout=15, retry_delay=60, max_retries=3,
                 log=None, on_pause=None, on_resume=None, is_closing=None, stats=None):
        if not url or not key:
            raise ValueError("PHONE_API_ENABLED=true cần PHONE_TOKEN_API_URL và PHONE_TOKEN_API_KEY trong .env")
        self.url = url
        self.key = key
        self.proxy = proxy or None
        self.timeout = timeout
        self.retry_delay = max(0.0, float(retry_delay))
        self.max_retries = max(1, int(max_retries))
        self.log = log or _default_log
        self.on_pause = on_pause
        self.on_resume = on_resume
        self.is_closing = is_closing  # callable: spider đang đóng thì không lấy token nữa
        self.stats = stats  # callable trả về Scrapy stats (hoặc None)

        self.token = None
        self.exp = None
        self.generation = 0
        self.failures = 0
        self.failed = False
        self.aborted = False  # dừng làm mới token vì spider đang đóng
        self._task = None

    def _inc(self, name):
        stats = self.stats() if self.stats else None
        if stats:
            stats.inc_value(f"phone_token/{name}")

    # ----------------------------------------------------------------- lấy token

    def fetch(self):
        """Gọi API token (đồng bộ). Thành công thì lưu token; lỗi thì raise PhoneTokenError."""
        proxies = {"http": self.proxy, "https": self.proxy} if self.proxy else None
        try:
            res = requests.get(
                self.url,
                headers={TOKEN_KEY_HEADER: self.key, "Accept": "application/json"},
                proxies=proxies,
                timeout=self.timeout,
            )
        except requests.RequestException as e:
            self._inc("error")
            raise PhoneTokenError(f"không gọi được API token: {e}") from e

        if res.status_code != 200:
            self._inc("error")
            raise PhoneTokenError(f"API token trả về HTTP {res.status_code}: {res.text[:200]}")
        try:
            data = res.json()
        except ValueError:
            self._inc("error")
            raise PhoneTokenError(f"API token trả về không phải JSON: {res.text[:200]!r}") from None

        token = data.get("token") if isinstance(data, dict) else None
        if not isinstance(token, str) or not token.strip():
            self._inc("error")
            raise PhoneTokenError("API token trả về token rỗng")

        exp = data.get("exp")
        try:
            exp = int(exp) if exp not in (None, "") else None
        except (TypeError, ValueError):
            exp = None
        if exp is not None and exp <= time.time():
            self._inc("error")
            raise PhoneTokenError(f"API token trả về token đã hết hạn (exp={exp})")

        self.token = token.strip()
        self.exp = exp
        self.generation += 1
        self._inc("ok")
        self.log("INFO", f"Đã lấy token API phone, hết hạn lúc {self._fmt_exp()}")

    def _fmt_exp(self):
        if self.exp is None:
            return "không rõ"
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.exp))

    def is_expired(self):
        return self.exp is not None and time.time() >= self.exp

    def mark_ok(self):
        """API phone chấp nhận token: reset bộ đếm số lần thử."""
        self.failures = 0

    # ------------------------------------------------------------- làm mới token

    async def renew(self, failed_generation=None):
        """Lấy token mới khi token hiện tại hết hạn / bị 401-403. Trả về True nếu đã có token dùng được.

        failed_generation: generation của token vừa bị từ chối. Nếu token đã được thay
        (generation khác) thì không lấy lại, chỉ cần gửi lại request.
        Nhiều callback gọi cùng lúc thì dùng chung 1 lượt làm mới.
        """
        if self.failed or self.aborted:
            return False
        if self._closing():
            self.aborted = True
            return False
        if failed_generation is not None and failed_generation != self.generation and self._task is None:
            return True
        if self._task is None:
            self._task = asyncio.ensure_future(self._renew_loop())
        return await asyncio.shield(self._task)

    def _closing(self):
        try:
            return bool(self.is_closing and self.is_closing())
        except Exception:
            return False

    async def _wait(self):
        """Chờ retry_delay giây; trả về False nếu spider bắt đầu đóng trong lúc chờ."""
        loop = asyncio.get_running_loop()
        end = loop.time() + self.retry_delay
        while True:
            if self._closing():
                return False
            left = end - loop.time()
            if left <= 0:
                return True
            await asyncio.sleep(min(1.0, left))

    async def _renew_loop(self):
        self._inc("renew")
        if self.on_pause:
            self.on_pause()
        try:
            while self.failures < self.max_retries:
                self.failures += 1
                self.log(
                    "WARNING",
                    f"Token API phone hết hạn hoặc bị từ chối. Tạm dừng spider, đợi {self.retry_delay:g}s rồi lấy "
                    f"token mới (lần {self.failures}/{self.max_retries})",
                )
                if not await self._wait():
                    self.aborted = True
                    self.log("INFO", "Spider đang dừng, bỏ qua việc lấy token mới")
                    return False
                try:
                    await asyncio.to_thread(self.fetch)
                    return True
                except PhoneTokenError as e:
                    self.log("WARNING", f"Lấy token lần {self.failures}/{self.max_retries} thất bại: {e}")
            self.failed = True
            self.log(
                "ERROR",
                f"Không lấy được token API phone hợp lệ sau {self.max_retries} lần thử. Dừng spider",
            )
            return False
        finally:
            self._task = None
            if self.on_resume:
                self.on_resume()


class PhoneApi:
    def __init__(self, url, token_provider, timeout=15, cache_ttl_hours=0, crawler=None):
        self.url = url or DEFAULT_URL
        self.tokens = token_provider
        self.timeout = timeout
        self.cache_ttl = max(0, int(cache_ttl_hours)) * 3600
        # Giữ crawler, không lấy crawler.stats ngay: từ Scrapy 2.19, stats chưa có
        # lúc tạo spider (from_crawler), chỉ có khi crawl đã bắt đầu
        self.crawler = crawler

    @property
    def stats(self):
        try:
            return self.crawler.stats if self.crawler else None
        except RuntimeError:
            return None

    @classmethod
    def from_crawler(cls, crawler, proxy=None, log=None):
        """Trả về None nếu PHONE_API_ENABLED=false.

        proxy: proxy của spider (USE_PROXY / CRAWL_PROXY hoặc -a proxy), dùng cho API token.
        log: hàm log(level, message), spider truyền vào để log có tag [HYDRA-MONITOR].
        """
        s = crawler.settings
        if not s.getbool("PHONE_API_ENABLED", True):
            return None
        api = cls(
            url=s.get("PHONE_API_URL"),
            token_provider=None,
            timeout=s.getint("PHONE_API_TIMEOUT", 15),
            cache_ttl_hours=s.getint("PHONE_CACHE_TTL_HOURS", 0),
            crawler=crawler,
        )
        api.tokens = TokenProvider(
            url=s.get("PHONE_TOKEN_API_URL"),
            key=s.get("PHONE_TOKEN_API_KEY"),
            proxy=proxy,
            timeout=s.getint("PHONE_API_TIMEOUT", 15),
            retry_delay=s.getfloat("PHONE_TOKEN_RETRY_DELAY", 60),
            max_retries=s.getint("PHONE_TOKEN_MAX_RETRIES", 3),
            log=log,
            on_pause=lambda: crawler.engine.pause(),
            on_resume=lambda: crawler.engine.unpause(),
            is_closing=lambda: engine_closing(crawler),
            stats=lambda: api.stats,
        )
        return api

    def _inc(self, name):
        if self.stats:
            self.stats.inc_value(f"phone_api/{name}")

    # --------------------------------------------------------------- token

    def init_token(self):
        """Lấy token lúc khởi động (1 lần). Lỗi thì raise PhoneTokenError."""
        self.tokens.fetch()

    def token_expired(self):
        return self.tokens.is_expired()

    async def renew_token(self, failed_generation=None):
        return await self.tokens.renew(failed_generation)

    def renew_aborted(self):
        """True nếu không làm mới token vì spider đang đóng (không phải lỗi token)."""
        return self.tokens.aborted

    # --------------------------------------------------------------- cache

    def get_cached(self, site, ad_id):
        if not self.cache_ttl:
            return None
        try:
            phone = get_redis().get(cache_key(site, ad_id))
        except Exception as e:  # Redis lỗi thì gọi API như bình thường
            logger.warning("Đọc cache phone lỗi: %s", e)
            return None
        if phone:
            self._inc("cache_hit")
        return phone or None

    def set_cached(self, site, ad_id, phone):
        if not self.cache_ttl or not phone:
            return
        try:
            get_redis().set(cache_key(site, ad_id), phone, ex=self.cache_ttl)
        except Exception as e:
            logger.warning("Ghi cache phone lỗi: %s", e)

    # ------------------------------------------------------------- request

    def build_request(self, ad_id, callback, errback, meta=None):
        meta = dict(meta or {})
        meta.update(
            phone_ad_id=str(ad_id),
            phone_token_gen=self.tokens.generation,
            download_timeout=self.timeout,
            handle_httpstatus_all=True,  # tự xử lý 401/403/404 trong callback
        )
        self._inc("request")
        return scrapy.Request(
            add_or_replace_parameter(self.url, "e", str(make_e(ad_id))),
            headers={**DEFAULT_HEADERS, "Authorization": f"Bearer {self.tokens.token}"},
            callback=callback,
            errback=errback,
            meta=meta,
            dont_filter=True,
        )

    def parse_response(self, response, site):
        """Trả về số điện thoại hoặc None. Raise PhoneAuthError khi 401/403."""
        ad_id = response.meta.get("phone_ad_id")
        status = response.status

        if status == 403 and is_phone_hidden(response.text):
            # Người bán ẩn số: token vẫn dùng được
            self.tokens.mark_ok()
            self._inc("hidden")
            return None

        if status in AUTH_ERROR_CODES:
            self._inc(f"http_{status}")
            raise PhoneAuthError(
                f"HTTP {status}: {response.text[:200]}",
                generation=response.meta.get("phone_token_gen"),
            )

        self.tokens.mark_ok()  # token được chấp nhận

        if status != 200:
            self._inc(f"http_{status}")
            self._inc("error")
            logger.warning("API phone %s:%s trả về HTTP %s", site, ad_id, status)
            return None

        try:
            data = json.loads(response.text)
        except ValueError:
            self._inc("bad_json")
            self._inc("error")
            logger.warning("API phone %s:%s trả về không phải JSON: %r", site, ad_id, response.text[:200])
            return None

        phone = clean_phone(data.get("phone")) if isinstance(data, dict) else None
        if not phone:
            self._inc("no_phone")
            return None

        self._inc("ok")
        self.set_cached(site, ad_id, phone)
        return phone

    def is_error_response(self, response):
        """True nếu response API phone là lỗi (HTTP lỗi, JSON sai): phone = null do lỗi, không phải
        người bán không có / ẩn số. Gọi sau parse_response (401/403 token đã raise ở đó)."""
        if response.status == 200:
            try:
                json.loads(response.text)
            except ValueError:
                return True
            return False
        return not (response.status == 403 and is_phone_hidden(response.text))

    def on_failure(self, failure, site):
        """Lỗi mạng, timeout (sau khi đã retry). Tin vẫn được gửi với phone = null."""
        self._inc("error")
        ad_id = failure.request.meta.get("phone_ad_id")
        logger.warning("Gọi API phone %s:%s lỗi: %s", site, ad_id, failure.getErrorMessage())
