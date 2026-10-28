"""Adapter riêng cho từng site. Chỉ bổ sung những gì phần chung không có."""

from chotot.parsers.base import BaseAdParser, to_float, to_int


class ChototParser(BaseAdParser):
    """chotot.com: đồ điện tử, đồ gia dụng... Đánh giá người bán, số tin đã bán đọc ở parser chung
    (BaseAdParser.seller) vì site khác cũng có thể có."""

    site = "chotot"


class XeParser(BaseAdParser):
    """xe.chotot.com: ô tô, xe máy, xe tải..."""

    site = "xe"

    def extra(self, item, ad, ad_params):
        specs = item["specs"]
        # ad.mileage / ad_params.mileage là MÃ khoảng (vd 3); số km thật nằm ở mileage_v2 (vd 12000)
        mileage = to_int(ad.get("mileage_v2")) or to_int(specs.get("mileage_v2"))
        specs.pop("mileage_v2", None)
        item["specs_labels"].pop("mileage_v2", None)
        if mileage is not None:
            specs["mileage"] = mileage
            item["specs_labels"]["mileage"] = "Số Km đã đi"
        else:
            specs.pop("mileage", None)



class VieclamtotParser(BaseAdParser):
    """vieclamtot.com: tin tuyển dụng. Không có price, dùng lương min/max."""

    site = "vieclamtot"

    def extra(self, item, ad, ad_params):
        item["salary_min"] = to_int(ad.get("min_salary"))
        item["salary_max"] = to_int(ad.get("max_salary"))
        item["salary_type"] = self.param_value(ad_params, "salary_type")

        seller = item["seller"]
        seller["company_name"] = seller.get("company_name") or self.param_value(ad_params, "company_name")
        item["specs"]["candidate_applied"] = ad.get("candidate_applied")
        item["specs_labels"]["candidate_applied"] = "Số ứng viên đã ứng tuyển"


class NhatotParser(BaseAdParser):
    """nhatot.com: bất động sản (mua bán, cho thuê).

    ad_params trả chữ để hiển thị ("53,5 m²", "2 phòng", "37,38 triệu/m²");
    ad có số chuẩn (size, rooms, toilets, price_million_per_m2) nên lấy số từ ad.
    """

    site = "nhatot"

    # specs id → (trường số trong ad, hàm đổi kiểu)
    NUMERIC_SPECS = {
        "size": ("size", to_float),
        "rooms": ("rooms", to_int),
        "toilets": ("toilets", to_int),
        "floornumber": ("floornumber", to_int),
    }

    def extra(self, item, ad, ad_params):
        specs, labels = item["specs"], item["specs_labels"]

        for key, (field, convert) in self.NUMERIC_SPECS.items():
            value = convert(ad.get(field))
            if value is not None:
                specs[key] = value
            elif key in specs and not isinstance(specs[key], (int, float)):
                specs.pop(key)
        if "size" in specs:
            specs["size_unit"] = ad.get("size_unit_string") or "m²"
            labels["size_unit"] = "Đơn vị diện tích"

        # "37,38 triệu/m²" → 37383180 (VNĐ/m²)
        specs.pop("price_m2", None)
        labels.pop("price_m2", None)
        price_m2 = to_float(ad.get("price_million_per_m2"))
        if price_m2:
            specs["price_per_m2"] = round(price_m2 * 1_000_000)
            labels["price_per_m2"] = "Giá/m² (VNĐ)"

        if ad.get("pty_project_name"):
            specs["project_name"] = ad["pty_project_name"]
            labels["project_name"] = "Dự án"
        if ad.get("projectid"):
            specs["project_id"] = ad["projectid"]
            labels["project_id"] = "Mã dự án"

        item["location"]["street"] = ad.get("street_name")
