import scrapy


class AdItem(scrapy.Item):
    """Schema chuẩn chung cho tin của chotot, xe.chotot, vieclamtot, nhatot.

    Thời gian (posted_at, updated_at) là timestamp mili giây.
    """

    # Định danh
    source = scrapy.Field()          # nền tảng: luôn là "chotot"
    site = scrapy.Field()            # chotot | xe | vieclamtot | nhatot
    ad_id = scrapy.Field()           # = list_id, khớp với ID trên URL
    internal_ad_id = scrapy.Field()  # = ad.ad_id (ID nội bộ Chợ Tốt)
    url = scrapy.Field()

    # Nội dung
    title = scrapy.Field()
    description = scrapy.Field()
    category = scrapy.Field()        # mã danh mục, vd "5040"
    category_name = scrapy.Field()
    condition = scrapy.Field()       # tình trạng: Mới / Đã sử dụng...

    # Giá
    price = scrapy.Field()           # số (VNĐ); việc làm = None
    price_text = scrapy.Field()      # chuỗi hiển thị, luôn có
    salary_min = scrapy.Field()      # chỉ việc làm
    salary_max = scrapy.Field()
    salary_type = scrapy.Field()     # vd "Theo tháng"

    # Vị trí: {address, city, city_v3, district, ward, ward_v3, lat, lng}
    location = scrapy.Field()

    # Thời gian
    posted_at = scrapy.Field()       # orig_list_time: lúc đăng lần đầu
    updated_at = scrapy.Field()      # list_time: lần đẩy tin gần nhất
    crawled_at = scrapy.Field()

    # Ảnh, video
    images = scrapy.Field()
    videos = scrapy.Field()

    # Người đăng: {account_id, account_oid, name, avatar, is_company, phone (số đầy đủ qua API),
    #              rating, total_rating, sold_ads, company_name, ...} — schema đầy đủ: chotot/schema.py
    seller = scrapy.Field()

    # Thông số riêng của từng loại tin: {id: giá trị}, {id: nhãn tiếng Việt}
    specs = scrapy.Field()
    specs_labels = scrapy.Field()
