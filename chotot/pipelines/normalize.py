"""Làm sạch dữ liệu tin. Dữ liệu lấy từ JSON nên đã có cấu trúc, chỉ cần làm sạch chữ.

Không tự đổi price_text thành số: với việc làm, price_text là khoảng lương
("10.000.000 đ - 12.000.000 đ/tháng"), đổi ra số sẽ sai.
"""

from itemadapter import ItemAdapter

from chotot.schema import conform_item
from chotot.utils.hydra import get_ad
from chotot.utils.normalizer import clean_multiline, clean_text, clean_url

TEXT_FIELDS = ("title", "category_name", "condition", "price_text", "salary_type")
DICT_TEXT_FIELDS = ("location", "seller")


def clean_dict(data):
    if not isinstance(data, dict):
        return {}
    return {k: clean_text(v) if isinstance(v, str) else v for k, v in data.items()}


def clean_specs(specs):
    if not isinstance(specs, dict):
        return {}
    result = {}
    for key, value in specs.items():
        if isinstance(value, str):
            value = clean_text(value)
        if key and value not in (None, ""):
            result[key] = value
    return result


def unique_urls(urls):
    if not urls:
        return []
    if isinstance(urls, str):
        urls = [urls]
    seen = []
    for url in urls:
        url = clean_text(url)
        if url and url.startswith("http") and url not in seen:
            seen.append(url)
    return seen


class NormalizePipeline:
    def process_item(self, item, spider=None):
        adapter = ItemAdapter(get_ad(item))

        for field in TEXT_FIELDS:
            if adapter.get(field) is not None:
                adapter[field] = clean_text(adapter[field])

        if adapter.get("description") is not None:
            adapter["description"] = clean_multiline(adapter["description"])

        if adapter.get("url"):
            adapter["url"] = clean_url(adapter["url"])

        if adapter.get("ad_id") is not None:
            adapter["ad_id"] = str(adapter["ad_id"]).strip()

        for field in DICT_TEXT_FIELDS:
            if adapter.get(field) is not None:
                adapter[field] = clean_dict(adapter[field])
        # seller, location đủ key theo schema (thiếu thì null), trước khi chống trùng tính fingerprint
        conform_item(adapter)

        if adapter.get("specs") is not None:
            adapter["specs"] = clean_specs(adapter["specs"])
            labels = adapter.get("specs_labels") or {}
            adapter["specs_labels"] = {k: v for k, v in labels.items() if k in adapter["specs"]}

        adapter["images"] = unique_urls(adapter.get("images"))

        # Đồng bộ url bên ngoài item Hydra với url đã làm sạch
        if isinstance(item, dict) and "item_data" in item and adapter.get("url"):
            item["item_data"]["url"] = adapter["url"]

        return item
