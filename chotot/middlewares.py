import random

DEFAULT_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0",
]


class RandomUserAgentMiddleware:
    def __init__(self, user_agents):
        self.user_agents = user_agents or DEFAULT_USER_AGENTS

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings.getlist("USER_AGENT_LIST"))

    def process_request(self, request, spider=None):
        request.headers["User-Agent"] = random.choice(self.user_agents)
        return None


class ProxyMiddleware:
    def __init__(self, enabled, proxy, stats=None):
        self.enabled = enabled and bool(proxy)
        self.proxy = proxy
        self.stats = stats

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            enabled=crawler.settings.getbool("USE_PROXY", False),
            proxy=crawler.settings.get("CRAWL_PROXY"),
            stats=crawler.stats,
        )

    def process_request(self, request, spider=None):
        if self.enabled and "proxy" not in request.meta:
            request.meta["proxy"] = self.proxy
            if self.stats:
                self.stats.inc_value("proxy/requests")
        return None