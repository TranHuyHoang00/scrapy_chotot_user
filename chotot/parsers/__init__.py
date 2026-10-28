"""Chọn parser theo domain của trang chi tiết."""

from urllib.parse import urlparse

from chotot.parsers.sites import ChototParser, NhatotParser, VieclamtotParser, XeParser

# Thứ tự quan trọng: xe.chotot.com phải đứng trước chotot.com
PARSERS = (
    ("xe.chotot.com", XeParser()),
    ("vieclamtot.com", VieclamtotParser()),
    ("nhatot.com", NhatotParser()),
    ("chotot.com", ChototParser()),
)

ALLOWED_DOMAINS = ["chotot.com", "vieclamtot.com", "nhatot.com"]

# Tên site → biến settings bật/tắt crawl (CRAWL_CHOTOT, CRAWL_XE, CRAWL_VIECLAMTOT, CRAWL_NHATOT)
SITES = ("chotot", "xe", "vieclamtot", "nhatot")


def site_setting(site):
    return f"CRAWL_{site.upper()}"


def site_of(url):
    """Tên site của link tin (chotot / xe / vieclamtot / nhatot), None nếu domain chưa có parser."""
    parser = get_parser(url)
    return parser.site if parser else None


def get_parser(url):
    host = (urlparse(url).hostname or "").lower()
    for domain, parser in PARSERS:
        if host == domain or host.endswith("." + domain):
            return parser
    return None
