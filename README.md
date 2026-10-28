# scrapy_chotot

Spider Scrapy thu thập tin đăng của hệ Chợ Tốt: **chotot.com, xe.chotot.com, nhatot.com, vieclamtot.com**. Spider viết theo quy ước **Hydra** (giống spider mẫu `scrapy_opensanctions`) để chạy trên Crawlab.

```
Kafka input (profile) ─▶ Spider ─▶ Pipeline ─▶ Kafka output: 4 topic
                          │         normalize → validation → chống trùng từng topic (Redis) → gửi Kafka
                          │           ├─ hydra-chotot-post_lss-h             thông tin bài đăng
                          │           ├─ hydra-images-incoming         ảnh bài đăng (1 message / ảnh)
                          │           ├─ hydra-profile-bio-chotot_lss-h      thông tin người đăng
                          │           └─ hydra-images-incoming-avatar  ảnh người đăng
                          ├─ tải trang người bán /user/<account_oid> (cache Redis)
                          ├─ 6 API số liệu người bán: theo dõi, đã bán, đang đăng, đánh giá, phản hồi chat (cache Redis)
                          ├─ lấy số điện thoại đầy đủ (API phone, cache Redis)
                          ├─ cập nhật crawled_time (Elasticsearch)
                          ├─ cập nhật source_status (API profile)
                          └─ log [HYDRA-MONITOR] ─▶ Kafka (hydra-spider-log)
```

**Mục lục**

1. [Yêu cầu](#1-yêu-cầu)
2. [Cài đặt](#2-cài-đặt)
3. [Cấu hình `.env`](#3-cấu-hình-env)
4. [Docker: Redis và Kafka local](#4-docker-redis-và-kafka-local)
5. [Chạy spider](#5-chạy-spider)
6. [Xem kết quả và log](#6-xem-kết-quả-và-log)
7. [Làm mới Redis / crawl lại từ đầu](#7-làm-mới-redis--crawl-lại-từ-đầu)
8. [Cập nhật code](#8-cập-nhật-code)
9. [Lỗi thường gặp](#9-lỗi-thường-gặp)
10. [Cách hoạt động](#10-cách-hoạt-động)
11. [Cấu trúc thư mục](#11-cấu-trúc-thư-mục)
12. [Còn cần xác nhận với team](#12-còn-cần-xác-nhận-với-team)

---

## 1. Yêu cầu

- Ubuntu/Linux, **Python 3.12+**, Git
- **Docker**: chạy Redis local (khi chưa có Redis dev của team) và Kafka local (khi test không dùng Kafka dev)
- **Kafka**: Kafka dev của team (gửi qua API HTTP) hoặc Kafka local bằng Docker (`KAFKA_LOCAL=true`) khi test, Kafka Hydra (broker) khi chạy thật
- **Proxy**, nếu máy phải đi qua proxy mới ra được Internet (ví dụ `http://172.20.148.40:3128`)

Nếu máy cần proxy, đặt biến này để dùng cho các lệnh cài đặt bên dưới:

```bash
export PROXY=http://172.20.148.40:3128
```

Máy không cần proxy thì bỏ các phần `-c http.proxy=$PROXY`, `-x $PROXY`, `--proxy $PROXY` trong các lệnh.

---

## 2. Cài đặt

### 2.1. Clone dự án

```bash
git -c http.proxy=$PROXY clone https://github.com/TranHuyHoang00/scrapy_chotot.git
cd scrapy_chotot
git config http.proxy $PROXY
```

### 2.2. Tạo môi trường Python (venv)

```bash
python3 -m venv --without-pip venv
source venv/bin/activate
curl -x $PROXY -sS --retry 5 https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python get-pip.py --proxy $PROXY
pip install --proxy $PROXY --retries 10 -r requirements.txt
rm get-pip.py
scrapy version
```

Mỗi lần mở terminal mới, cần kích hoạt lại venv:

```bash
cd scrapy_chotot
source venv/bin/activate
```

### 2.3. Cài Docker (bỏ qua nếu đã có, hoặc đã có Redis dev)

```bash
sudo apt -o Acquire::http::Proxy=$PROXY -o Acquire::https::Proxy=$PROXY update
sudo apt -o Acquire::http::Proxy=$PROXY -o Acquire::https::Proxy=$PROXY install -y docker.io docker-compose-v2
sudo usermod -aG docker $USER
```

Để Docker tải image qua proxy:

```bash
sudo mkdir -p /etc/systemd/system/docker.service.d
printf '[Service]\nEnvironment="HTTP_PROXY=%s"\nEnvironment="HTTPS_PROXY=%s"\nEnvironment="NO_PROXY=localhost,127.0.0.1"\n' $PROXY $PROXY \
  | sudo tee /etc/systemd/system/docker.service.d/http-proxy.conf
sudo systemctl daemon-reload && sudo systemctl restart docker
```

**Đăng xuất rồi đăng nhập lại Ubuntu** để nhận quyền docker, sau đó kiểm tra:

```bash
docker run --rm hello-world
```

---

## 3. Cấu hình `.env`

```bash
cp .env.example .env
```

File `.env` phải nằm ở **thư mục gốc dự án** (cùng chỗ với `scrapy.cfg`). File này đã có trong `.gitignore`, **không commit**. Trên Crawlab, khai báo các biến này trong phần Environment của spider thay cho file `.env`.

Biến nào không khai báo trong `.env` sẽ dùng giá trị mặc định trong `chotot/settings.py`. `.env.example` liệt kê đầy đủ các biến.

### 3.1. Cấu hình test trên Kafka dev (dùng hằng ngày)

```ini
# Kafka dev qua API HTTP
KAFKA_MODE=http
KAFKA_HTTP_URL=<URL API publish-message của Kafka dev>
KAFKA_HTTP_API_KEY=<API key>
KAFKA_TOPIC_POST=hydra-chotot-post_lss-h
KAFKA_TOPIC_POST_IMAGES=hydra-images-incoming
KAFKA_TOPIC_PROFILE_BIO=hydra-profile-bio-chotot_lss-h
KAFKA_TOPIC_PROFILE_AVATAR=hydra-images-incoming-avatar
KAFKA_LOGGING_ENABLED=false

# Redis local (docker compose)
REDIS_HOST=localhost
REDIS_PORT=6379

# API lấy token cho API số điện thoại
PHONE_TOKEN_API_URL=<URL API lấy token>
PHONE_TOKEN_API_KEY=<giá trị header X-Session-Keeper-Key>

# Proxy, nếu máy cần (dùng cho cả crawl web và API phone)
USE_PROXY=true
CRAWL_PROXY=http://172.20.148.40:3128
```

### 3.2. Cấu hình test với Kafka local (khi Kafka dev lỗi)

Dựng Kafka bằng Docker (mục 4.2) rồi bật `KAFKA_LOCAL`. Không cần sửa `KAFKA_MODE`, `KAFKA_BOOTSTRAP_SERVERS`, SSL: `KAFKA_LOCAL=true` tự ghi đè thành `broker`, `localhost:9092`, không SSL. Tắt lại (`false`) là quay về Kafka dev như cũ.

```ini
KAFKA_LOCAL=true
KAFKA_TOPIC_POST=hydra-chotot-post_lss-h
KAFKA_TOPIC_POST_IMAGES=hydra-images-incoming
KAFKA_TOPIC_PROFILE_BIO=hydra-profile-bio-chotot_lss-h
KAFKA_TOPIC_PROFILE_AVATAR=hydra-images-incoming-avatar
KAFKA_LOGGING_ENABLED=true      # log [HYDRA-MONITOR] cũng đi vào Kafka local (topic hydra-spider-log)

REDIS_HOST=localhost
REDIS_PORT=6379

PHONE_TOKEN_API_URL=<URL API lấy token>
PHONE_TOKEN_API_KEY=<giá trị header X-Session-Keeper-Key>
```

### 3.3. Cấu hình chạy thật trên Hydra

```ini
KAFKA_MODE=broker
KAFKA_BOOTSTRAP_SERVERS=<host:port>,<host:port>
KAFKA_SECURITY_PROTOCOL=SSL
KAFKA_TOPIC_INPUT=hydra-profile-scheduler-chotot
KAFKA_TOPIC_POST=hydra-chotot-post_lss-h
KAFKA_TOPIC_POST_IMAGES=hydra-images-incoming
KAFKA_TOPIC_PROFILE_BIO=hydra-profile-bio-chotot_lss-h
KAFKA_TOPIC_PROFILE_AVATAR=hydra-images-incoming-avatar
KAFKA_LOGGING_ENABLED=true

REDIS_HOST=<redis của team>
ES_HOST=<elasticsearch>
END_POINT_API=<api profile>
API_KEY_GLOBAL=<api key>

PHONE_TOKEN_API_URL=<URL API lấy token>
PHONE_TOKEN_API_KEY=<giá trị header X-Session-Keeper-Key>
```

Đặt 3 file chứng chỉ Kafka vào `chotot/ssl/`: `ca.pem`, `key.pem`, `certificate.pem`. Thư mục này đã được `.gitignore`.

### 3.4. Ý nghĩa các biến

**Kafka**

| Biến                                   | Mặc định                         | Ý nghĩa                                                                                                                                                                         |
| -------------------------------------- | -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `KAFKA_LOCAL`                          | `false`                          | `true`: dùng Kafka local (mục 4.2). Ghi đè `KAFKA_MODE=broker`, `KAFKA_BOOTSTRAP_SERVERS=localhost:KAFKA_LOCAL_PORT`, `KAFKA_SECURITY_PROTOCOL=PLAINTEXT`. Tên topic giữ nguyên |
| `KAFKA_LOCAL_PORT`                     | `9092`                           | Cổng Kafka local trên máy (docker compose cũng đọc). Đổi khi cổng 9092 đã bị dùng                                                                                               |
| `KAFKA_LOCAL_BOOTSTRAP_SERVERS`        | `localhost:<KAFKA_LOCAL_PORT>`   | Địa chỉ Kafka local, chỉ cần khi Kafka chạy ở máy khác                                                                                                                          |
| `KAFKA_UI_PORT`                        | `8080`                           | Cổng giao diện Kafka UI                                                                                                                                                         |
| `KAFKA_MODE`                           | `broker`                         | `http`: gửi qua API publish-message (Kafka dev). `broker`: kết nối thẳng Kafka (Hydra)                                                                                          |
| `KAFKA_TOPIC_INPUT`                    | `hydra-profile-scheduler-chotot` | Topic spider **đọc** profile cần crawl (chỉ dùng ở `broker`)                                                                                                                    |
| `KAFKA_TOPIC_POST`                     | `hydra-chotot-post_lss-h`        | Topic **thông tin bài đăng** (mục 10.9)                                                                                                                                         |
| `KAFKA_TOPIC_POST_IMAGES`              | `hydra-images-incoming`          | Topic **ảnh bài đăng**, 1 message / ảnh                                                                                                                                         |
| `KAFKA_TOPIC_PROFILE_BIO`              | `hydra-profile-bio-chotot_lss-h` | Topic **thông tin người đăng**                                                                                                                                                  |
| `KAFKA_TOPIC_PROFILE_AVATAR`           | `hydra-images-incoming-avatar`   | Topic **ảnh người đăng** (avatar)                                                                                                                                               |
| `KAFKA_TOPIC_LOG`                      | `hydra-spider-log`               | Topic nhận log `[HYDRA-MONITOR]`                                                                                                                                                |
| `KAFKA_LOGGING_ENABLED`                | `true`                           | Gửi log `[HYDRA-MONITOR]` lên Kafka. Test thì đặt `false`                                                                                                                       |
| `KAFKA_HTTP_URL`, `KAFKA_HTTP_API_KEY` |                                  | URL và key của API Kafka dev (chỉ dùng ở `http`)                                                                                                                                |
| `KAFKA_HTTP_BATCH_SIZE`                | `50`                             | Số message gom vào 1 request API                                                                                                                                                |
| `KAFKA_HTTP_TIMEOUT`                   | `30`                             | Số giây chờ API phản hồi                                                                                                                                                        |
| `KAFKA_BOOTSTRAP_SERVERS`              | `localhost:9092`                 | Địa chỉ Kafka broker (chỉ dùng ở `broker`)                                                                                                                                      |
| `KAFKA_SECURITY_PROTOCOL`              | `SSL`                            | `SSL` hoặc `PLAINTEXT`                                                                                                                                                          |
| `KAFKA_GROUP_ID`                       | `group-chotot`                   | Consumer group khi đọc topic input                                                                                                                                              |

**Redis, Elasticsearch, API profile**

| Biến                                                     | Mặc định                        | Ý nghĩa                                                                                                                                            |
| -------------------------------------------------------- | ------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `REDIS_HOST`, `REDIS_PORT`, `REDIS_DB`, `REDIS_PASSWORD` | `localhost`, `6379`, `0`, trống | Redis lưu dữ liệu chống trùng                                                                                                                      |
| `DEDUP_TTL_DAYS`                                         | `30`                            | Dấu vân tay chống trùng hết hạn sau N ngày: quá hạn thì message được gửi lại 1 lần để đồng bộ, Redis tự dọn key cũ. `0` = không hết hạn (mục 10.4) |
| `ES_HOST`                                                | trống                           | Elasticsearch để ghi `crawled_time`. Trống thì bỏ qua                                                                                              |
| `END_POINT_API`, `API_KEY_GLOBAL`                        | trống                           | API cập nhật trạng thái profile (alive, dead). Trống thì bỏ qua                                                                                    |

**API số điện thoại** (xem [10.5](#105-số-điện-thoại-api-phone))

| Biến                      | Mặc định                                                 | Ý nghĩa                                                                                            |
| ------------------------- | -------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `PHONE_API_ENABLED`       | `true`                                                   | Gọi API lấy số đầy đủ. `false` thì `seller.phone = null`                                           |
| `PHONE_API_URL`           | `https://gateway.chotot.com/v1/private/ad-listing/phone` | Endpoint API, gọi `GET ?e=<ad_id đã mã hóa RSA + base64>` (mục 10.5)                               |
| `PHONE_TOKEN_API_URL`     | trống                                                    | API lấy Bearer token cho API phone (GET). **Bắt buộc** khi `PHONE_API_ENABLED=true`                |
| `PHONE_TOKEN_API_KEY`     | trống                                                    | Giá trị header `X-Session-Keeper-Key` gửi tới API token. **Bắt buộc** khi `PHONE_API_ENABLED=true` |
| `PHONE_TOKEN_RETRY_DELAY` | `60`                                                     | Token hết hạn: số giây chờ trước mỗi lần lấy lại token (spider tạm dừng trong lúc chờ)             |
| `PHONE_TOKEN_MAX_RETRIES` | `3`                                                      | Số lần lấy lại token tối đa; vẫn không được thì dừng spider                                        |
| `PHONE_API_TIMEOUT`       | `15`                                                     | Số giây chờ API phản hồi                                                                           |
| `PHONE_API_DELAY`         | `1.5`                                                    | Số giây nghỉ giữa 2 lần gọi API (AutoThrottle có thể tự tăng)                                      |
| `PHONE_API_CONCURRENCY`   | `2`                                                      | Số request API song song tối đa                                                                    |
| `PHONE_CACHE_TTL_HOURS`   | `168`                                                    | Giữ số đã lấy trong Redis bao nhiêu giờ (khỏi gọi lại API), `0` = không cache                      |

**Trang người bán**

| Biến                             | Mặc định | Ý nghĩa                                                                                                                                    |
| -------------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `SELLER_PROFILE_ENABLED`         | `true`   | Tải trang người bán để lấy `name`, `avatar`, `banner`, `address`, `joined_at`, `is_active`… (mục 10.6). `false` thì các field này = `null` |
| `SELLER_PROFILE_CACHE_TTL_HOURS` | `72`     | Giữ thông tin người bán trong Redis bao nhiêu giờ (khỏi tải lại), `0` = không cache                                                        |

**API số liệu người bán**

| Biến                           | Mặc định | Ý nghĩa                                                                                                                                                                                                                                                                                                                     |
| ------------------------------ | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SELLER_STATS_ENABLED`         | `true`   | Gọi 6 API public lấy `followers`, `followings`, `sold_ads`, `live_ads`, `rating`, `total_rating`, `total_rating_as_seller`, `total_rating_as_buyer`, `response_rate`, `response_rate_text`, `response_time`, `response_time_text` (mục 10.7). `false`: `followers`, `followings` = `null`, 4 field còn lại lấy từ trang tin |
| `SELLER_STATS_CACHE_TTL_HOURS` | `24`     | Giữ số liệu trong Redis bao nhiêu giờ, `0` = không cache                                                                                                                                                                                                                                                                    |
| `SELLER_STATS_DELAY`           | `0.5`    | Số giây nghỉ giữa 2 request API số liệu (slot tốc độ riêng, tách khỏi API phone)                                                                                                                                                                                                                                            |
| `SELLER_STATS_CONCURRENCY`     | `2`      | Số request API số liệu song song tối đa                                                                                                                                                                                                                                                                                     |

**Crawl**

| Biến                             | Mặc định                         | Ý nghĩa                                                                         |
| -------------------------------- | -------------------------------- | ------------------------------------------------------------------------------- |
| `USE_PROXY`                      | `false`                          | Bật hoặc tắt proxy khi crawl web                                                |
| `CRAWL_PROXY`                    | trống                            | Địa chỉ proxy, vd `http://172.20.148.40:3128` hoặc `http://user:pass@host:port` |
| `CHOTOT_START_URL`               | `https://www.chotot.com/mua-ban` | Trang danh sách bắt đầu crawl                                                   |
| `CHOTOT_MAX_PAGES`               | `5`                              | Số trang danh sách tối đa (`/mua-ban`, `?page=2`…). Lưu ý: `0` = chỉ trang 1    |
| `CHOTOT_MAX_ITEMS_PER_PAGE`      | `0`                              | Số tin tối đa mỗi trang (tính trên tin của site đang bật), `0` = lấy hết        |
| `CRAWL_CHOTOT`                   | `true`                           | Crawl tin chotot.com. `false`: gặp tin chotot.com thì bỏ qua, không tải         |
| `CRAWL_XE`                       | `true`                           | Crawl tin xe.chotot.com                                                         |
| `CRAWL_VIECLAMTOT`               | `true`                           | Crawl tin vieclamtot.com (đang bị Cloudflare chặn: đặt `false`)                 |
| `CRAWL_NHATOT`                   | `true`                           | Crawl tin nhatot.com                                                            |
| `TARGET_ITEM_COUNT`              | `0`                              | Dừng sau N tin hợp lệ, `0` = không giới hạn                                     |
| `DOWNLOAD_DELAY`                 | `1.5`                            | Số giây nghỉ giữa 2 request                                                     |
| `CONCURRENT_REQUESTS`            | `8`                              | Số request song song tối đa (tổng)                                              |
| `CONCURRENT_REQUESTS_PER_DOMAIN` | `4`                              | Số request song song tối đa cho mỗi domain                                      |

Proxy áp dụng cho request crawl web và API phone. Kafka, Redis, Elasticsearch, API profile luôn kết nối trực tiếp.

**Log**

| Biến                     | Mặc định | Ý nghĩa                                                    |
| ------------------------ | -------- | ---------------------------------------------------------- |
| `LOG_LEVEL`              | `INFO`   | Mức log: `DEBUG`, `INFO`, `WARNING`, `ERROR`               |
| `MONITOR_ENABLED`        | `true`   | Ghi log ra file `logs/` và in bảng tổng kết cuối lượt chạy |
| `MONITOR_PROGRESS_EVERY` | `50`     | In tiến độ sau mỗi N tin                                   |

---

## 4. Docker: Redis và Kafka local

`docker-compose.yml` có 2 nhóm:

| Nhóm        | Container                                              | Bật bằng                           |
| ----------- | ------------------------------------------------------ | ---------------------------------- |
| Redis       | `chotot_redis`                                         | `docker compose up -d` (luôn chạy) |
| Kafka local | `chotot_kafka`, `chotot_kafka_init`, `chotot_kafka_ui` | thêm `--profile kafka`             |

### 4.1. Redis (chống trùng)

Spider dùng Redis để nhớ những tin đã gửi, lần chạy sau không gửi lại tin không đổi. **Redis bắt buộc phải chạy trước khi chạy spider.**

Chưa có Redis dev của team thì chạy Redis local bằng Docker.

```bash
docker compose up -d --remove-orphans   # bật Redis (--remove-orphans: dọn container cũ không còn dùng)
docker compose ps                       # chotot_redis phải ở trạng thái Up (healthy)
```

Kiểm tra Redis hoạt động:

```bash
docker exec chotot_redis redis-cli ping     # trả về PONG
```

Tắt Redis:

```bash
docker compose stop redis                 # tắt, giữ dữ liệu
docker compose rm -sf redis               # xóa container, vẫn giữ dữ liệu (volume redis_data)
docker volume rm chotot_redis_data        # xóa toàn bộ dữ liệu chống trùng + cache (sau khi rm)
```

Không dùng `docker compose down -v` nếu đang dùng Kafka local: lệnh này xóa cả volume `kafka_data`.

Dữ liệu lưu trong volume `redis_data`, tắt máy không mất. Redis tự bật lại khi Docker khởi động (`restart: unless-stopped`).

Khi có Redis dev của team: sửa `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD` trong `.env`, không cần chạy Docker nữa.

### 4.2. Kafka local

Dùng khi Kafka dev lỗi hoặc muốn test độc lập. Kafka 1 node (`apache/kafka:3.9.0`, chế độ KRaft, không cần Zookeeper, không SSL) và giao diện web Kafka UI.

**Bật** (Redis + Kafka):

```bash
docker compose --profile kafka up -d
docker compose --profile kafka ps        # chotot_kafka phải Up (healthy), chotot_kafka_init đã Exited (0)
docker logs chotot_kafka_init            # danh sách topic đã tạo
```

`chotot_kafka_init` tạo sẵn 6 topic theo tên trong `.env` (`KAFKA_TOPIC_INPUT`, `KAFKA_TOPIC_LOG` và 4 topic output `KAFKA_TOPIC_POST`, `_POST_IMAGES`, `_PROFILE_BIO`, `_PROFILE_AVATAR`) rồi tự thoát. Topic khác được Kafka tự tạo khi có message đầu tiên.

**Dùng cho spider:** đặt `KAFKA_LOCAL=true` trong `.env` (mục 3.2). Đầu log sẽ có dòng `Kafka: LOCAL localhost:9092 (KAFKA_LOCAL=true)`.

| Kết nối                       | Địa chỉ                                 |
| ----------------------------- | --------------------------------------- |
| Spider, script trên máy       | `localhost:9092` (`KAFKA_LOCAL_PORT`)   |
| Container khác trong compose  | `kafka:29092`                           |
| Kafka UI (xem topic, message) | http://localhost:8080 (`KAFKA_UI_PORT`) |

**Xem message bằng dòng lệnh:**

```bash
# 5 bài đăng mới gửi lên topic bài đăng (đổi --topic để xem 3 topic còn lại)
docker exec chotot_kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 \
  --topic hydra-chotot-post_lss-h --from-beginning --max-messages 5

# Số message trong mỗi topic
docker exec chotot_kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:29092
```

**Tắt / xóa:**

```bash
docker compose --profile kafka stop kafka kafka-ui     # tắt Kafka, Redis vẫn chạy
docker compose --profile kafka rm -sf kafka kafka-init kafka-ui   # xóa container Kafka, giữ dữ liệu; Redis vẫn chạy
docker volume rm chotot_kafka_data                     # xóa toàn bộ message Kafka local (sau khi rm)
```

Message giữ 7 ngày. Kafka local tự bật lại khi Docker khởi động (`restart: unless-stopped`); không cần nữa thì `stop`.

---

## 5. Chạy spider

Luôn chạy tại **thư mục gốc dự án**, đã kích hoạt venv và Redis đang chạy.

### 5.1. Test trên Kafka dev

Ở `KAFKA_MODE=http`, API chỉ **đẩy** được message, không đọc được topic input, nên spider tự tạo 1 profile test từ `start_url`.

Chạy thử ít tin (khuyên dùng lần đầu):

```bash
scrapy crawl chotot -a start_url=https://www.chotot.com/mua-ban -s CLOSESPIDER_ITEMCOUNT=10
```

Chạy theo cấu hình trong `.env` (`CHOTOT_MAX_PAGES`, `TARGET_ITEM_COUNT`…):

```bash
scrapy crawl chotot -a start_url=https://www.chotot.com/mua-ban
```

Bỏ `-a start_url=...` thì spider dùng `CHOTOT_START_URL` trong `.env`.

### 5.2. Test với Kafka local

Bật Kafka local (mục 4.2) và `KAFKA_LOCAL=true`. Kafka local đọc được topic input nên test được cả 2 cách:

```bash
# Cách 1: tự tạo profile từ start_url, không đọc topic input
scrapy crawl chotot -a start_url=https://www.chotot.com/mua-ban -s CLOSESPIDER_ITEMCOUNT=10

# Cách 2: giống Hydra: đẩy profile vào topic input rồi spider đọc ra
python scripts/seed_profiles.py --pages 1
scrapy crawl chotot -s CLOSESPIDER_ITEMCOUNT=10
```

Xem kết quả trên Kafka UI (http://localhost:8080 → Topics → `hydra-chotot-post_lss-h`, `hydra-images-incoming`, `hydra-profile-bio-chotot_lss-h`, `hydra-images-incoming-avatar` → Messages) hoặc lệnh `kafka-console-consumer.sh` ở mục 4.2.

### 5.3. Test 1 tin cụ thể (ghi item ra file JSON)

Đưa link tin vào `-a detail_url`. Spider vào thẳng trang tin, chạy đủ các bước như chạy thật rồi ghi **đúng các message sẽ gửi lên 4 topic Kafka** ra file trong `logs/`:

```
trang chi tiết tin → trang người bán → API số liệu người bán → API phone → normalize → validation → dựng message 4 topic → ghi file
```

```bash
# 1 tin
scrapy crawl chotot -a detail_url=https://xe.chotot.com/mua-ban-oto-quan-cau-giay-ha-noi/135193883.htm

# nhiều tin: cách nhau dấu phẩy (message các tin nối vào cùng list của từng topic)
scrapy crawl chotot -a detail_url="https://www.chotot.com/.../111210710.htm,https://www.nhatot.com/.../128000000.htm"

# tự đặt tên file
scrapy crawl chotot -a detail_url=<url> -a output=logs/xe_test.json

# ghi file VÀ gửi Kafka thật (theo KAFKA_MODE / KAFKA_LOCAL trong .env)
scrapy crawl chotot -a detail_url=<url> -a send_kafka=1
```

Cuối log có dòng chỉ ra file:

```
[DETAIL-TEST] Đã ghi 1 tin vào: logs/detail_135401339_20261027_095139.json (hydra-chotot-post_lss-h: 1, hydra-images-incoming: 3, hydra-profile-bio-chotot_lss-h: 1, hydra-images-incoming-avatar: 1)
```

File có dạng (mỗi topic là 1 list message, đúng như gửi lên Kafka):

```json
{
  "hydra-chotot-post_lss-h": [ {...} ],
  "hydra-images-incoming": [ {...}, {...}, {...} ],
  "hydra-profile-bio-chotot_lss-h": [ {...} ],
  "hydra-images-incoming-avatar": [ {...} ]
}
```

Xem nhanh: `cat logs/detail_135193883_*.json`, hoặc `python -m json.tool <file>`.

|                                                   | Luồng test `detail_url`                                                                                        | Chạy thường                  |
| ------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- | ---------------------------- |
| Trang danh sách, Kafka input                      | bỏ qua                                                                                                         | có                           |
| Trang người bán, API số liệu người bán, API phone | **có** (dùng cả cache Redis)                                                                                   | có                           |
| normalize, validation                             | **có**                                                                                                         | có                           |
| Chống trùng Redis (cả 4 topic)                    | **bỏ qua**: không kiểm tra, không đánh dấu. Test lại bao nhiêu lần cũng ra item, không ảnh hưởng lần chạy thật | có                           |
| Gửi Kafka                                         | **không** (trừ khi `-a send_kafka=1`)                                                                          | có                           |
| Log [HYDRA-MONITOR] lên Kafka                     | không (trừ khi `-a send_kafka=1`)                                                                              | theo `KAFKA_LOGGING_ENABLED` |
| Ghi file `logs/detail_*.json`                     | có                                                                                                             | không                        |
| Cập nhật crawled_time (ES), source_status         | không                                                                                                          | có                           |

Không có file / log `[DETAIL-TEST] Không có item nào để ghi`: tin bị loại ở validation (`Loại vì ...` trong bảng tổng kết), trang tin lỗi (`[DETAIL-TEST] Không tải được tin ...`), tin đã ẩn (`Không có dữ liệu tin`) hoặc domain chưa có parser.

Cần Redis đang chạy (cache số điện thoại / trang người bán). Không có token API phone thì thêm `-s PHONE_API_ENABLED=false` (`seller.phone = null`).

### 5.4. Chạy trên Hydra / Crawlab

```bash
scrapy crawl chotot
```

Spider đọc profile từ `KAFKA_TOPIC_INPUT` (do Hydra scheduler đẩy vào), crawl rồi đẩy kết quả lên 4 topic output (`KAFKA_TOPIC_POST`, `KAFKA_TOPIC_POST_IMAGES`, `KAFKA_TOPIC_PROFILE_BIO`, `KAFKA_TOPIC_PROFILE_AVATAR`).

Muốn tự đẩy 1 profile test vào topic input (chỉ dùng được với `KAFKA_MODE=broker`):

```bash
python scripts/seed_profiles.py --pages 2      # đẩy profile
python scripts/seed_profiles.py --dry-run      # chỉ in profile ra, không đẩy
scrapy crawl chotot -s CLOSESPIDER_ITEMCOUNT=30
```

### 5.5. Tham số dòng lệnh

| Tham số                          | Ý nghĩa                                                                                 |
| -------------------------------- | --------------------------------------------------------------------------------------- |
| `-a detail_url=<url>[,<url>...]` | Test tin cụ thể, ghi message 4 topic ra `logs/detail_*.json`, không gửi Kafka (mục 5.3) |
| `-a send_kafka=1`                | Dùng với `detail_url`: gửi Kafka thật, ngoài việc ghi file                              |
| `-a output=<file>`               | Dùng với `detail_url`: đường dẫn file JSON thay cho tên tự đặt                          |
| `-a start_url=<url>`             | Chạy test với 1 profile tự tạo, không đọc Kafka input                                   |
| `-a proxy=<url>`                 | Proxy cho lần chạy này, ghi đè `.env`                                                   |
| `-a topic=<tên>`                 | Đọc profile từ topic khác `KAFKA_TOPIC_INPUT`                                           |
| `-a check_lag=1`                 | Ghi log số message còn tồn trong topic input                                            |
| `-s CLOSESPIDER_ITEMCOUNT=<N>`   | Dừng sau N tin hợp lệ (ghi đè `TARGET_ITEM_COUNT`)                                      |
| `-s CHOTOT_MAX_PAGES=<N>`        | Số trang danh sách cho lần chạy này                                                     |
| `-s LOG_LEVEL=DEBUG`             | Xem log chi tiết hơn                                                                    |
| `-o logs/test.json`              | Xuất thêm các item ra file JSON để xem nhanh (không thay cho Kafka)                     |

Dừng spider đang chạy: bấm `Ctrl + C` **một lần** và chờ spider gửi nốt message còn lại rồi tự thoát. Bấm 2 lần sẽ dừng ngay và có thể mất lô message cuối.

---

## 6. Xem kết quả và log

Mỗi lần chạy tạo 1 file log mới: `logs/chotot_<năm tháng ngày>_<giờ phút giây>.log`. Thư mục `logs/` tự tạo nếu chưa có và đã được `.gitignore`.

Các dòng cần xem:

- Đầu log: cho biết spider đã nhận đúng chế độ và topic:
  - `Kafka: http` / `Kafka: broker` / `Kafka: LOCAL localhost:9092 (KAFKA_LOCAL=true)`
  - `Kafka (broker) topic output: hydra-chotot-post_lss-h, hydra-images-incoming, hydra-profile-bio-chotot_lss-h, hydra-images-incoming-avatar`
  - `API phone: bật, cache 168 giờ`, `Trang người bán: bật, cache 72 giờ`
- Trong lúc chạy:
  - `Trang N: X link tin`: số tin lấy được trên mỗi trang danh sách.
  - `Đã gửi N message lên topic ... qua API`: gửi Kafka thành công (chỉ ở `KAFKA_MODE=http`; broker/local không log từng lần gửi, xem dòng `Kafka:` ở bảng tổng kết).
  - `Gửi Kafka thất bại ...`: gửi lỗi, kèm mã HTTP và nội dung lỗi.
- Cuối log có bảng tổng kết:

```
===== Kết thúc spider 'chotot' sau 85.3s (đủ số item mục tiêu) =====
Request: 25 gửi, 25 phản hồi, 0 thử lại, 0 bị robots.txt chặn
Item: 10 hợp lệ, 2 bị loại
  - Loại vì chotot/missing_title: 2
Chống trùng tin: 10 mới, 0 cập nhật, 0 trùng
Kafka: 52 gửi, 52 thành công, 0 thất bại
  - hydra-chotot-post_lss-h: 10 gửi, 10 thành công, 0 thất bại (bỏ qua vì không đổi: 0)
  - hydra-images-incoming: 34 gửi, 34 thành công, 0 thất bại (bỏ qua vì không đổi: 0)
  - hydra-profile-bio-chotot_lss-h: 4 gửi, 4 thành công, 0 thất bại (bỏ qua vì không đổi: 6)
  - hydra-images-incoming-avatar: 4 gửi, 4 thành công, 0 thất bại (bỏ qua vì không đổi: 6)
API phone: 8 gọi, 6 có số, 1 không có số, 1 ẩn số, 0 lỗi, 2 lấy từ cache
Token API phone: 1 lần lấy thành công, 0 lỗi, 0 lần làm mới
Trang người bán: 7 tải, 7 có dữ liệu, 0 không có dữ liệu, 0 lỗi, 3 lấy từ cache
API số liệu người bán: 7 người bán, 7 có dữ liệu, 0 lỗi mọi API, 3 lấy từ cache
Bỏ qua tin của site đã tắt: vieclamtot: 12
Theo site:
  - chotot: 6
  - xe: 4
```

Xem log mới nhất:

```bash
ls -t logs/*.log | head -1 | xargs tail -n 40
```

Dữ liệu trên Kafka dev xem bằng công cụ của team (4 topic output, mục 10.9).

Dọn file log cũ hơn 7 ngày:

```bash
find logs -name '*.log' -mtime +7 -delete
```

---

## 7. Làm mới Redis / crawl lại từ đầu

Redis lưu dấu vân tay đã gửi **riêng cho từng topic**, mỗi message 1 key, tự hết hạn sau `DEDUP_TTL_DAYS` ngày (cách hoạt động: mục 10.4):

| Key                                                          | Topic                                                                     |
| ------------------------------------------------------------ | ------------------------------------------------------------------------- |
| `dedup:post:<site>:<post_id>` (vd `dedup:post:xe:135401339`) | bài đăng                                                                  |
| `dedup:images:<site>:<post_id>`                              | ảnh bài đăng                                                              |
| `dedup:bio:<account_oid>`                                    | người đăng (lưu cả message đã gửi, để giữ giá trị cũ khi lần sau API lỗi) |
| `dedup:avatar:<account_oid>`                                 | avatar                                                                    |

Message có trong Redis và nội dung không đổi sẽ **không được gửi lại** lên topic đó. Muốn gửi lại (vd test lại việc đẩy Kafka) thì xóa dữ liệu này.

Phiên bản trước lưu chống trùng dạng hash (`dedup:chotot`, `dedup_bio`...). Các key đó không còn dùng, xóa được bằng lệnh "Xóa toàn bộ dữ liệu chống trùng" bên dưới.

Ngoài ra, số điện thoại lấy từ API được cache trong các key `phone:<site>:<ad_id>`, tự hết hạn sau `PHONE_CACHE_TTL_HOURS` giờ. Muốn gọi lại API cho mọi tin thì xóa cache này:

```bash
docker exec chotot_redis redis-cli --scan --pattern 'phone:*' | xargs -r docker exec -i chotot_redis redis-cli del
```

Thông tin trang người bán cache trong các key `seller:<account_oid>` (`SELLER_PROFILE_CACHE_TTL_HOURS`). Muốn tải lại:

```bash
docker exec chotot_redis redis-cli --scan --pattern 'seller:*' | xargs -r docker exec -i chotot_redis redis-cli del
```

Số liệu người bán (theo dõi, đã bán, đánh giá...) cache trong các key `seller_stats:<account_oid>` (`SELLER_STATS_CACHE_TTL_HOURS`). Muốn gọi lại API:

```bash
docker exec chotot_redis redis-cli --scan --pattern 'seller_stats:*' | xargs -r docker exec -i chotot_redis redis-cli del
```

**Xem số tin đã lưu:**

```bash
docker exec chotot_redis redis-cli --scan --pattern 'dedup:post:chotot:*' | wc -l   # số tin chotot đã gửi
docker exec chotot_redis redis-cli --scan --pattern 'dedup:bio:*' | wc -l           # số người bán đã gửi
docker exec chotot_redis redis-cli ttl dedup:post:xe:135401339                      # số giây còn lại trước khi hết hạn
```

**Xóa toàn bộ dữ liệu chống trùng** (lần chạy sau gửi lại mọi tin):

```bash
docker exec chotot_redis redis-cli --scan --pattern 'dedup*' | xargs -r docker exec -i chotot_redis redis-cli del
```

**Chỉ xóa 1 site**, vd xe (bài đăng + ảnh):

```bash
docker exec chotot_redis redis-cli --scan --pattern 'dedup:*:xe:*' | xargs -r docker exec -i chotot_redis redis-cli del
```

**Chỉ xóa 1 tin**, vd tin xe `135401339` (lần sau gửi lại bài đăng + ảnh của tin này):

```bash
docker exec chotot_redis redis-cli del dedup:post:xe:135401339 dedup:images:xe:135401339
```

**Chỉ xóa 1 người bán** (lần sau gửi lại bio + avatar):

```bash
docker exec chotot_redis redis-cli del dedup:bio:b9f3fc8c7281753d9c0276e16ec33d31 dedup:avatar:b9f3fc8c7281753d9c0276e16ec33d31
```

**Xóa sạch Redis** (Redis local chỉ dùng cho dự án này):

```bash
docker exec chotot_redis redis-cli flushdb
```

Dùng Redis dev của team (không phải Docker) thì thay `docker exec chotot_redis redis-cli` bằng `redis-cli -h <REDIS_HOST> -p <REDIS_PORT> -a <REDIS_PASSWORD>`. **Không dùng `flushdb` trên Redis dùng chung**, vì sẽ xóa dữ liệu của dự án khác.

---

## 8. Cập nhật code

```bash
cd scrapy_chotot
source venv/bin/activate
git pull origin main
pip install --proxy $PROXY -r requirements.txt
docker compose up -d --remove-orphans                  # Redis
docker compose --profile kafka up -d                   # thêm dòng này nếu dùng Kafka local
```

Nếu commit mới có sửa `docker-compose.yml`, lệnh `up -d` tự tạo lại container đã thay đổi.

Sau khi pull, so sánh `.env.example` với `.env` của bạn để thêm biến mới (nếu cần đổi khác mặc định):

```bash
diff <(grep -o '^[A-Z_]*' .env.example | sort) <(grep -o '^[A-Z_]*' .env | sort)
```

Dòng bắt đầu bằng `<` là biến có trong `.env.example` mà `.env` chưa có.

---

## 9. Lỗi thường gặp

| Lỗi                                                                     | Nguyên nhân / cách xử lý                                                                                                                                                                                                                                                       |
| ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `scrapy: command not found`                                             | Chưa kích hoạt venv: `source venv/bin/activate`                                                                                                                                                                                                                                |
| `Unknown command: crawl`                                                | Không chạy tại thư mục gốc dự án (nơi có `scrapy.cfg`)                                                                                                                                                                                                                         |
| `ConnectionError` / `Connection refused` Redis                          | Redis chưa chạy: `docker compose up -d`                                                                                                                                                                                                                                        |
| `KAFKA_MODE=http cần KAFKA_HTTP_URL và KAFKA_HTTP_API_KEY`              | Thiếu 2 biến này trong `.env`                                                                                                                                                                                                                                                  |
| `Gửi ... qua API thất bại: HTTP 401/403`                                | Sai `KAFKA_HTTP_API_KEY`                                                                                                                                                                                                                                                       |
| `Gửi ... qua API thất bại: ... timed out`                               | API Kafka dev không phản hồi; tăng `KAFKA_HTTP_TIMEOUT` hoặc kiểm tra mạng                                                                                                                                                                                                     |
| `NoBrokersAvailable` khi `KAFKA_LOCAL=true`                             | Kafka local chưa chạy hoặc chưa healthy: `docker compose --profile kafka up -d`, chờ `chotot_kafka` healthy. Kiểm tra `KAFKA_LOCAL_PORT` khớp với cổng đã map                                                                                                                  |
| `port is already allocated` khi bật Kafka local                         | Cổng 9092/8080 đã bị dùng: đổi `KAFKA_LOCAL_PORT` / `KAFKA_UI_PORT` trong `.env` rồi `up -d` lại                                                                                                                                                                               |
| `Total source: 0`                                                       | Ở `broker`: topic input chưa có profile, hoặc group đã đọc hết message. Ở test: thêm `-a start_url=...`                                                                                                                                                                        |
| Lỗi SSL khi kết nối Kafka broker                                        | Kiểm tra 3 file trong `chotot/ssl/` và `KAFKA_SECURITY_PROTOCOL`                                                                                                                                                                                                               |
| Không tải được trang nào (timeout, connection refused)                  | Máy cần proxy: `USE_PROXY=true`, `CRAWL_PROXY=...`                                                                                                                                                                                                                             |
| `Chống trùng tin: 0 mới, ... N trùng`, Kafka 0 gửi                      | Tin đã gửi ở lần trước, không đổi. Xóa dữ liệu chống trùng (mục 7)                                                                                                                                                                                                             |
| `Không có __NEXT_DATA__` / `Không có dữ liệu tin`                       | Trang bị chặn (vd Cloudflare với nhatot) hoặc tin đã bị ẩn                                                                                                                                                                                                                     |
| `Không tải được tin ...: HTTP 403` (vd vieclamtot.com)                  | Cloudflare chặn truy cập tự động (trang "Just a moment...", header `Server: cloudflare`, `cf-ray`). Không phải lỗi proxy hay User-Agent. Tạm tắt site đó (`CRAWL_VIECLAMTOT=false`) để các site khác chạy bình thường; muốn crawl lại cần quyền truy cập chính thức từ Chợ Tốt |
| `PHONE_API_ENABLED=true cần PHONE_TOKEN_API_URL và PHONE_TOKEN_API_KEY` | Thiếu 2 biến này trong `.env`. Không cần số điện thoại thì đặt `PHONE_API_ENABLED=false`                                                                                                                                                                                       |
| `Không lấy được token API phone: ...`, spider dừng ngay khi khởi động   | API token lỗi, sai `PHONE_TOKEN_API_KEY`, trả token rỗng hoặc đã hết hạn. Kiểm tra URL, key, proxy                                                                                                                                                                             |
| `Không lấy được token API phone hợp lệ sau N lần thử`, spider dừng      | Token hết hạn giữa chừng và lấy lại `PHONE_TOKEN_MAX_RETRIES` lần đều không được. Tin chưa lấy được số không bị đánh dấu chống trùng, lần sau được crawl lại                                                                                                                   |
| `API phone: ... N lỗi` nhiều                                            | API chậm hoặc bị giới hạn tần suất: tăng `PHONE_API_DELAY`, giảm `PHONE_API_CONCURRENCY`, kiểm tra proxy                                                                                                                                                                       |
| `Chưa có parser cho domain`                                             | Link tin thuộc domain mới, cần thêm parser (mục 10.3)                                                                                                                                                                                                                          |
| `cannot import name 'classproperty' from 'kafka.util'`                  | Cài lẫn 2 thư viện Kafka. Chạy: `pip uninstall -y kafka-python kafka-python-ng` rồi `pip install -r requirements.txt`                                                                                                                                                          |
| `Crawler.stats is not set yet` khi khởi động spider                     | Code cũ chạy trên Scrapy 2.19+. `git pull origin main` để lấy bản đã sửa                                                                                                                                                                                                       |
| `permission denied ... docker.sock`                                     | Chưa đăng xuất/đăng nhập lại sau khi thêm user vào nhóm docker                                                                                                                                                                                                                 |

---

## 10. Cách hoạt động

### 10.1. Đầu vào: profile

- **Hydra (`broker`)**: spider đọc topic `KAFKA_TOPIC_INPUT`. Mỗi message là 1 profile, `seed_url`/`url` là trang tổng hợp `https://www.chotot.com/mua-ban`. URL không thuộc chotot.com thì dùng `CHOTOT_START_URL`.
- **Test (`-a start_url` hoặc `KAFKA_MODE=http`)**: spider tự tạo 1 profile test, không đọc Kafka.
- **Test tin cụ thể (`-a detail_url`)**: profile test, vào thẳng trang tin, ghi message ra file (mục 5.3).
- **Kafka local (`KAFKA_LOCAL=true`)**: như Hydra, đọc topic input của Kafka local (đẩy profile bằng `scripts/seed_profiles.py`), hoặc dùng `-a start_url` để bỏ qua topic input.

Trường profile dùng đến: `profile_id` (cập nhật crawled_time, source_status), `seed_url`/`url` (trang cần crawl). Có thể thêm (không bắt buộc): `max_pages`, `max_items_per_page`. Message gửi Kafka **không** lấy `name`, `type`, `channel_name`, `tenants` từ profile: các field nhận dạng là giá trị cố định (mục 10.9).

### 10.2. Crawl

1. Ghi `crawled_time` của profile vào Elasticsearch (nếu có `ES_HOST`).
2. **Trang danh sách**: `/mua-ban`, `/mua-ban?page=2`… tới `max_pages`. Gặp trang không có tin thì dừng sớm.
   Link tin lấy từ `<li> … <a href=".../<ID>.htm">`. Không dựa vào class CSS vì class sinh tự động, đổi mỗi lần web build lại.
   Link có thể thuộc xe.chotot.com, nhatot.com, vieclamtot.com. Tin của site đã tắt (`CRAWL_<SITE>=false`) bị bỏ ngay ở đây, không tải trang tin; trang chỉ toàn tin site tắt vẫn sang trang sau. Đầu log có dòng `Site crawl: chotot, xe, nhatot | TẮT: vieclamtot`.
   Trang 1 có tin thì profile được đánh dấu `crawled, alive` (1 lần/lượt chạy); trang 1 lỗi 4xx thì `dead`.
   **Thứ tự**: request trang danh sách có `priority=-10` (`LIST_PRIORITY`), thấp hơn mọi request của tin (trang tin, trang người bán, API, phone = 0). Spider crawl xong tin của trang hiện tại rồi mới sang trang sau: trang 1 → tin trang 1 → trang 2 → … nên item lên Kafka đều từ đầu. Không có priority thì hàng đợi LIFO của Scrapy chạy hết các trang danh sách trước, tin bị dồn tới cuối, Kafka rất lâu mới nhận lô đầu.
3. **Trang chi tiết**: chọn parser theo domain, đọc JSON trong `<script id="__NEXT_DATA__">` (`props.initialState.adView.adInfo`), tạo `AdItem` theo schema chung.
4. **Trang người bán**: tải `chotot.com/user/<account_oid>` (1 lần/người bán, cache Redis) để điền `seller.address`, `joined_at`, `is_active`… (mục 10.6).
5. **Số liệu người bán**: gọi 6 API public (1 lượt/người bán, cache Redis) để điền `followers`, `followings`, `sold_ads`, `live_ads`, `rating`, `total_rating` (mục 10.7).
6. **Số điện thoại**: gọi API phone để điền `seller.phone` rồi mới đưa tin vào pipeline (mục 10.5).

### 10.3. Parser từng site

| Domain           | Parser             | Phần riêng                                                                                     |
| ---------------- | ------------------ | ---------------------------------------------------------------------------------------------- |
| `chotot.com`     | `ChototParser`     | không có phần riêng (đánh giá, số tin đã bán đọc ở parser chung)                               |
| `xe.chotot.com`  | `XeParser`         | số km thật từ `mileage_v2` (`mileage` gốc chỉ là mã khoảng)                                    |
| `vieclamtot.com` | `VieclamtotParser` | không có `price`; dùng `salary_min/max/type`; `company_name` lấy thêm từ `ad_params` nếu thiếu |
| `nhatot.com`     | `NhatotParser`     | diện tích, số phòng, tầng dạng số; giá/m² (VNĐ); dự án; tên đường                              |

Phần chung (`BaseAdParser`) đọc mọi field người bán có thể có ở bất kỳ site nào (đánh giá, `sold_ads`, `seller_info.live_ads`, công ty): site nào có dữ liệu thì tự được điền, không có thì `null`. Cuối cùng `seller`, `location` được ép theo schema cố định (mục 10.10).

Thêm site mới: viết 1 class kế thừa `BaseAdParser` trong `chotot/parsers/sites.py`, thêm domain vào `PARSERS` và `ALLOWED_DOMAINS` trong `chotot/parsers/__init__.py`.

### 10.4. Pipelines

| Thứ tự | Pipeline         | Việc làm                                                                                                                                                                                                   |
| ------ | ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 100    | `normalize`      | Làm sạch chữ, bỏ trùng ảnh, bỏ thông số rỗng                                                                                                                                                               |
| 200    | `validation`     | Loại tin thiếu `ad_id`, `url`, `title` (`VALIDATION_RULES` trong `settings.py`). **Không bắt buộc `price`**: tin cho tặng, thỏa thuận, không ghi giá vẫn giữ với `price = null`; giá sai/âm thì đặt `null` |
| 300    | `redis_dedup`    | Dựng message 4 topic (`chotot/hydra_topics.py`), quyết định **riêng từng topic** có cần gửi không (xem "Chống trùng" bên dưới). Không topic nào cần gửi thì bỏ item                                        |
| 400    | `kafka_pipeline` | Gửi message lên 4 topic; Kafka xác nhận thành công thì đánh dấu chống trùng                                                                                                                                |

#### Chống trùng

Mỗi message đã gửi được nhớ bằng 1 **dấu vân tay** (mã sha1 của nội dung) trong Redis. Lần crawl sau tính lại dấu vân tay: **giống** thì không gửi, **khác hoặc chưa có** thì gửi. So **riêng từng topic**:

| Topic        | So theo       | Gửi khi                                                                                                                                                                            |
| ------------ | ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| bài đăng     | `post_id`     | tin mới, hoặc nội dung tin đổi (giá, mô tả, ảnh, tên người bán...). Tin chỉ được "đẩy lại" (`updated_at` đổi) thì không gửi lại                                                    |
| ảnh bài đăng | `post_id`     | cùng lúc với bài đăng (gửi lại đủ ảnh của tin)                                                                                                                                     |
| người đăng   | `account_oid` | thông tin người bán đổi (vd `followers`, `sold_ads`, `response_rate`). **Không** tính `joined_duration` ("7 năm 4 tháng" tự đổi mỗi tháng). Nhiều tin cùng người bán chỉ gửi 1 lần |
| avatar       | `account_oid` | link avatar đổi                                                                                                                                                                    |

**Quy tắc quan trọng:**

1. **Chỉ đánh dấu khi Kafka đã nhận.** Dấu vân tay chỉ được lưu sau khi Kafka xác nhận gửi thành công **mọi** message của topic đó (vd đủ 3 ảnh của tin). Gửi lỗi (dù chỉ 1 ảnh), hoặc spider bị tắt ngang trước khi Kafka xác nhận (Ctrl+C 2 lần, Crawlab kill, mất mạng; ở `KAFKA_MODE=http` message còn trong lô chưa gửi) thì topic đó **chưa được đánh dấu**: lần chạy sau gửi lại riêng topic đó, topic đã thành công không gửi lại.
2. **Không ghi đè dữ liệu tốt bằng `null` khi API lỗi.** Field người bán bị `null` **do lỗi** (trang người bán lỗi, API số liệu lỗi, API phone lỗi/timeout) thì message người đăng dùng **giá trị đã gửi lần trước** cho field đó (log `Người bán ...: giữ N field đã gửi trước`). Nếu chỉ field lỗi mà các field khác không đổi thì không gửi lại. `null` thật (người bán ẩn số, không có banner, API trả không có số liệu) vẫn gửi `null`.
3. **Hết hạn sau `DEDUP_TTL_DAYS` ngày** (mặc định 30). Quá hạn thì message được gửi lại 1 lần dù không đổi, để bên nhận đồng bộ lại nếu đã mất dữ liệu; Redis tự dọn key của tin đã gỡ. `0` = không hết hạn.
4. Bên nhận nên **cập nhật đè theo khóa** (bài đăng: `post_id`; ảnh: `post_id` + `url`; người đăng, avatar: `profile_id`) vì message có thể được gửi lại (nội dung đổi, gửi lỗi, hết hạn).

Luồng test `-a detail_url`: bỏ qua chống trùng, không đánh dấu.

### 10.5. Số điện thoại (API phone)

`__NEXT_DATA__` chỉ có số đã che (`097567****`), nên số đầy đủ lấy qua API riêng. Toàn bộ phần gọi API nằm trong `chotot/utils/phone_api.py`; đổi API thì chỉ sửa file này.

```
GET PHONE_API_URL?e=<ad_id đã mã hóa>
Authorization: Bearer <token>
→ {"phone": "0523149999"}
```

**Token** không đặt trong `.env` mà lấy từ API token:

```
GET PHONE_TOKEN_API_URL
X-Session-Keeper-Key: PHONE_TOKEN_API_KEY
→ {"token": "...", "cookie": "privateToken", "exp": 1792727075, "at": 1792654459950}
```

`exp` là thời điểm hết hạn (giây), `at` là lúc cấp (mili giây). API token đi qua proxy giống crawl web.

- **Lúc khởi động**: gọi API token 1 lần, trước khi đọc profile. Lỗi (không gọi được, HTTP khác 200, không phải JSON, `token` rỗng, `exp` đã qua) thì log `[HYDRA-MONITOR]` ERROR và **dừng spider** (lý do `phone_token_error`).
- **Token hết hạn giữa chừng** (quá `exp`, hoặc API phone trả 401/403): **tạm dừng spider** (không lấy request mới), đợi `PHONE_TOKEN_RETRY_DELAY` giây rồi lấy token mới, rồi gọi lại API phone cho các tin đang chờ. Mỗi lần lấy token lỗi, hoặc token mới vẫn bị 401/403, tính là 1 lần thử. Thử tối đa `PHONE_TOKEN_MAX_RETRIES` lần (đếm lại từ 0 sau mỗi lần API phone chấp nhận token); vẫn không được thì **dừng spider** (lý do `phone_token_error`). Nếu spider đang đóng (đủ item, Ctrl+C…) thì bỏ qua việc lấy token, tin đang chờ số không được gửi để lần sau crawl lại.

Tham số `e` **không phải** `ad_id` dạng số, mà là `ad_id` đã mã hóa (giống cách web Chợ Tốt gọi API):

1. `ad_id` = ID trên URL tin (`list_id`), vd `.../135193883.htm` → `135193883`.
2. Mã hóa RSA (padding PKCS#1 v1.5) bằng public key `PROD_PUBKEY` nhúng trong `phone_api.py` (cần thư viện `cryptography`).
3. Mã hóa base64, gắn vào URL: `?e=<chuỗi base64>`. Hàm thực hiện: `make_e(ad_id)`.

Padding PKCS#1 v1.5 có thành phần ngẫu nhiên nên mỗi lần gọi, `e` của cùng một tin sẽ khác nhau; đó là bình thường.

`PROD_PUBKEY` là **public key** của Chợ Tốt (web công khai), không phải bí mật, nên để trong code thay vì `.env`. Nếu Chợ Tốt đổi key (API trả lỗi dù token còn hạn), cập nhật lại `PROD_PUBKEY` trong `chotot/utils/phone_api.py`.

- Dùng chung cho mọi site (chotot, xe, nhatot, vieclamtot).
- Gọi **bất đồng bộ** qua Scrapy (không chặn crawl), đi qua proxy giống crawl web. Tốc độ riêng cho `gateway.chotot.com`: `PHONE_API_DELAY`, `PHONE_API_CONCURRENCY`.
- Số lấy được cache trong Redis `phone:<site>:<ad_id>` (`PHONE_CACHE_TTL_HOURS`); tin đã có trong cache không gọi API lại.
- Kết quả:

| Trường hợp                                                           | Xử lý                                                                                                                                                              |
| -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Có số                                                                | `seller.phone = "0523149999"`, gửi tin                                                                                                                             |
| Người bán ẩn số: HTTP 403 `{"message":"forbidden - phone hidden"}`   | `seller.phone = null`, **vẫn gửi tin**, đếm vào "ẩn số". Không coi là lỗi token                                                                                    |
| Không có số, HTTP lỗi khác 401/403, lỗi mạng/timeout (sau khi retry) | `seller.phone = null`, **vẫn gửi tin**. Lần sau lấy được số thì chống trùng thấy nội dung đổi và gửi bản mới                                                       |
| 401, hoặc 403 không phải "phone hidden" (token hết hạn/sai)          | Tạm dừng spider, lấy token mới rồi gọi lại (xem phần Token ở trên). Thử hết số lần vẫn lỗi thì **dừng spider**; tin chưa có số không được gửi để lần sau crawl lại |

### 10.6. Trang người bán

Trang chi tiết tin chỉ có tên, avatar… của người bán. Thông tin khác lấy từ trang người bán `https://www.chotot.com/user/<account_oid>` (`seller.url`), trong `__NEXT_DATA__` → `props.pageProps.profile`. Dùng chung cho mọi site. Code nằm trong `chotot/utils/seller_profile.py`.

| `seller`             | Lấy từ `profile`                                                                                                               | Ví dụ                                                         |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------- |
| `name`               | `full_name` (không có thì giữ tên từ trang tin)                                                                                | `"siêu thị ô tô Nhật Bắc"`                                    |
| `avatar`             | `avatar` (không có thì giữ avatar từ trang tin)                                                                                | `"https://cdn.chotot.com/.../916493c2....jpg"`                |
| `banner`             | `banner` (ảnh bìa; nhiều tài khoản không có → `null`)                                                                          | `"https://cdn.chotot.com/uac_banner/31634981"`                |
| `address`            | `address`                                                                                                                      | `"QL3 Lộc Hà Mai Lâm Đông Anh"`                               |
| `about`              | `description` (giới thiệu)                                                                                                     | `"Mua bán trao đổi tất cả smartphone cũ mới..."`              |
| `gender`             | `gender` (thường rỗng → `null`)                                                                                                | `null`                                                        |
| `joined_at`          | `create_time` (giây, không có thì `start_time`) → ms                                                                           | `1456318239000`                                               |
| `joined_duration`    | HTML trang: `<p>Đã tham gia: <!-- --> 8 năm 5 tháng</p>` (đọc theo chữ nhãn, không theo class; không có trong `__NEXT_DATA__`) | `"8 năm 5 tháng"` (nguyên văn, web tự tính tới lúc tải trang) |
| `profile_updated_at` | `update_time` (giây) → ms                                                                                                      | `1589256726000`                                               |
| `is_active`          | `is_active`                                                                                                                    | `true`                                                        |
| `phone_verified`     | `phone_verified` (`"t"`/`"f"`)                                                                                                 | `true`                                                        |
| `email_verified`     | `email_verified` (`"t"`/`"f"`)                                                                                                 | `false`                                                       |

`banner` khác `iconBanners.banner` (banner "tải app" chung của web, không lấy). Không lấy `id_number`, `id_place` (dữ liệu cá nhân nhạy cảm) và `phone`, `old_phone` (luôn rỗng). `rating`, `follow` trong `initialState` là state trình duyệt tải sau (luôn 0) nên không dùng.

- Thứ tự: trang chi tiết tin → trang người bán → API số liệu người bán (mục 10.7) → API phone → pipeline.
- Mỗi `account_oid` chỉ tải 1 lần: kết quả cache trong bộ nhớ (lượt chạy) và Redis `seller:<account_oid>` (`SELLER_PROFILE_CACHE_TTL_HOURS`). Nhiều tin cùng người bán đang chờ thì dùng chung 1 lần tải. Cache ghi bởi code cũ (thiếu field mới thêm, vd `banner`) được coi như chưa có và tải lại, không cần xóa tay.
- Tài khoản đã đặt tên rút gọn (`profile.alias`): `/user/<account_oid>` chuyển hướng (301/302) sang `/user/<alias>`, vd `/user/0349e5b3…` → `/user/HDMobileHP01`. Spider đi theo chuyển hướng (log `Trang người bán ... được chuyển hướng tới ...`). `seller.url` vẫn giữ dạng `/user/<account_oid>` (luôn dùng được, không đổi khi người bán đổi alias). Profile được tìm theo `account_oid` trong `__NEXT_DATA__` của trang đích.
- Tải lỗi (HTTP 4xx/5xx, lỗi mạng/timeout sau khi retry, trang không có `profile`; log `Tải trang người bán ... lỗi: HTTP ...`): các field trên để `null`, **vẫn gửi tin**, không cache kết quả lỗi. Lần sau lấy được thì chống trùng thấy nội dung đổi và gửi bản mới.
- Đi qua proxy và tốc độ crawl web (`DOWNLOAD_DELAY`, `CONCURRENT_REQUESTS_PER_DOMAIN`). `SELLER_PROFILE_ENABLED=false` thì không tải, các field = `null`.

### 10.7. API số liệu người bán

Sau trang người bán, spider gọi **lần lượt 6 API public** (không cần token) theo `account_oid`, dùng chung mọi site. Code: `chotot/utils/seller_stats.py` (sửa `APIS` khi đổi / thêm API).

| API (`https://gateway.chotot.com/v1/public/...`)                      | Lấy                                                                                        | `seller`                                                                                                                                                                                           |
| --------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `social/followers/count/<oid>`                                        | `result`                                                                                   | `followers` (người theo dõi)                                                                                                                                                                       |
| `social/followings/countall/<oid>`                                    | `result`                                                                                   | `followings` (đang theo dõi)                                                                                                                                                                       |
| `solds_by/<oid>?limit=1&offset=0`                                     | `paging.total`                                                                             | `sold_ads` (tin đã bán)                                                                                                                                                                            |
| `theia/<oid>?limit=1&page=1`                                          | `total` (không có thì `paging.total`)                                                      | `live_ads` (tin đang đăng)                                                                                                                                                                         |
| `ratings/<oid>?post_type=all&page=1&limit=1&version=2.0.0&rejected=1` | `average_rating`, `total_rating`, `total_review_as_seller`, `total_review_as_buyer`        | `rating`, `total_rating` (tổng), `total_rating_as_seller`, `total_rating_as_buyer`                                                                                                                 |
| `https://gateway.chotot.com/v2/public/chat/user/get/<oid>` (**v2**)   | `result.response_rate` (0.93), `response_rate_text`, `response_time`, `response_time_text` | `response_rate` (`93`: %, làm tròn xuống như web; `-1` → `null`), `response_rate_text` (`"Thỉnh thoảng"`), `response_time` (nguyên giá trị API, vd `-1`), `response_time_text` (`"Phản hồi chậm"`) |

- `sold_ads`, `live_ads`, `rating`, `total_rating`: **ưu tiên API**; API lỗi / không có giá trị thì giữ giá trị lấy từ trang tin. Các field còn lại (`followers`, `followings`, `total_rating_as_seller/buyer`, `response_rate`…): chỉ có từ API, lỗi thì `null`.
- Mỗi API lỗi riêng (HTTP lỗi, timeout, không phải JSON) chỉ làm field của API đó lỗi, vẫn gọi API tiếp theo. Log `API số liệu người bán <oid>: API <tên> ...`; bảng tổng kết có số lỗi theo từng API.
- Mỗi người bán gọi 1 lượt (6 request), cache bộ nhớ + Redis `seller_stats:<account_oid>` (`SELLER_STATS_CACHE_TTL_HOURS`, mặc định 24 giờ). Lỗi mọi API thì không cache.
- Tốc độ riêng (download slot `seller-stats`: `SELLER_STATS_DELAY`, `SELLER_STATS_CONCURRENCY`), tách khỏi API phone dù cùng domain `gateway.chotot.com`.
- `total_rating` là tổng đánh giá (cả vai trò người bán lẫn người mua): vd 23 = `total_rating_as_seller` 21 + `total_rating_as_buyer` 2.

### 10.8. Gửi Kafka

- `KAFKA_MODE=http`: gom message theo lô `KAFKA_HTTP_BATCH_SIZE`, gọi
  `POST KAFKA_HTTP_URL?apikey=KAFKA_HTTP_API_KEY` với body `{"topic": "...", "message": "[{...}, {...}]"}`. `message` là **chuỗi JSON** của mảng message. Lô cuối gửi khi spider kết thúc.
- `KAFKA_MODE=broker`: gửi thẳng Kafka broker, mỗi message 1 lần gửi.
- Key message: `post_id` (bài đăng, ảnh bài đăng), `account_oid` (người đăng, avatar).

### 10.9. Message gửi Kafka (4 topic)

Dựng ở `chotot/hydra_topics.py`. Cấu trúc và thứ tự key đúng theo file mẫu của từng topic. Thời gian là timestamp **mili giây**.

**Giá trị cố định** (bên Kafka cấp để nhận dạng dữ liệu): field không lấy được từ dữ liệu crawl / API dùng giá trị trong mẫu, giống nhau ở mọi message, **ghi thẳng trong `hydra_topics.py`, không đặt trong `.env`**:

| Hằng số                             | Giá trị                                                                                                      | Dùng ở                                             |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------ | -------------------------------------------------- |
| `TENANT_IDS`                        | `["9c70933aff6b2a6d08c687a6cbb6b765"]`                                                                       | `tenant_ids`: bài đăng, ảnh bài đăng, avatar       |
| `TENANTS`                           | `[{"id": "9c70933aff6b2a6d08c687a6cbb6b765", "name": "HY Global", "project": "global", "category_ids": []}]` | `_tenants` (bài đăng), `tenants` (ảnh bài đăng)    |
| `OWNER_ID`                          | `"6aca663149b2732717abdd050062fe6e"`                                                                         | `owner_id` (bài đăng), `profile_id` (ảnh bài đăng) |
| `OWNER_NAME`, `OWNER_TYPE`          | `"Chợ tốt"`, `"chotot"`                                                                                      | bài đăng                                           |
| `CHANNEL_NAME`                      | `"market"`                                                                                                   | cả 4 topic                                         |
| `CRAWLER_SYSTEM`, `type`, `docType` | `"api-crawler"`, `"post"`, `"Post"`                                                                          | bài đăng                                           |
| `image_type`                        | `"photo"` / `"avatar"`                                                                                       | ảnh bài đăng / avatar                              |
| `profile_type`                      | `"person"`                                                                                                   | người đăng                                         |
| mặc định `about`, `gender`          | `""`, `"FEMALE"`                                                                                             | người đăng, khi trang người bán không có dữ liệu   |

#### `hydra-chotot-post_lss-h`: thông tin bài đăng (1 message / tin)

```json
{
  "url": "https://xe.chotot.com/mua-ban-oto-huyen-yen-my-hung-yen/135401339.htm",
  "owner_name": "Chợ tốt",
  "owner_id": "6aca663149b2732717abdd050062fe6e",
  "updated_time": 1792656920589,
  "tenant_ids": ["9c70933aff6b2a6d08c687a6cbb6b765"],
  "inserted_time": 1793069499409,
  "post_id": "135401339",
  "created_time": 1792656917000,
  "flag_crawl_created_time": 1,
  "entry_rowkey": "a7b22f86dde3dd42eb157bdd2fb1c2d2",
  "type": "post",
  "docType": "Post",
  "crawler_system": "api-crawler",
  "_tenants": [
    {
      "id": "9c70933aff6b2a6d08c687a6cbb6b765",
      "name": "HY Global",
      "project": "global",
      "category_ids": []
    }
  ],
  "owner_type": "chotot",
  "data": {
    "source": "chotot",
    "site": "xe",
    "ad_id": "135401339",
    "internal_ad_id": "179614832",
    "account_id": 13125056,
    "account_oid": "b9f3fc8c7281753d9c0276e16ec33d31",
    "title": "...",
    "description": "...",
    "category": "2010",
    "category_name": "Ô tô",
    "condition": "Đã sử dụng",
    "price": 95000000,
    "price_text": "95.000.000 đ",
    "salary_min": null,
    "salary_max": null,
    "salary_type": null,
    "location": {
      "address": "...",
      "city": "Hưng Yên",
      "city_v3": "Hưng Yên",
      "district": "Huyện Yên Mỹ",
      "ward": "...",
      "ward_v3": "Xã Yên Mỹ",
      "street": null,
      "region_id": 1006,
      "area_id": 100604,
      "ward_id": 4127,
      "lat": 20.882452,
      "lng": 106.03757
    },
    "posted_at": 1792656917000,
    "updated_at": 1792656920589,
    "crawled_at": 1793069487331,
    "attachments": [
      {
        "width": "",
        "url": "https://cdn.chotot.com/...jpg",
        "ratio": "",
        "entry_id": "a7b22f86dde3dd42eb157bdd2fb1c2d2",
        "height": "",
        "target_id": "",
        "duration": "",
        "type": "photo",
        "target_url": "",
        "description": "",
        "format": ""
      }
    ],
    "videos": [],
    "specs": { "carbrand": "Kia", "...": "..." },
    "specs_labels": { "carbrand": "Hãng", "...": "..." }
  },
  "author_id": "b9f3fc8c7281753d9c0276e16ec33d31",
  "channel_name": "market",
  "author_name": "Trường Giang"
}
```

| Field                                 | Nguồn                                                                                                                                                                                                                                                                 |
| ------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `post_id`                             | `list_id`                                                                                                                                                                                                                                                             |
| `entry_rowkey`                        | MD5 (hex) của `"<post_id>_<account_oid>"`, vd `md5("135401339_b9f3fc8c7281753d9c0276e16ec33d31")` = `a7b22f86dde3dd42eb157bdd2fb1c2d2`. Tin không có `account_oid`: `md5("<post_id>")`. Dùng chung cho ảnh bài đăng (`entry_rowkey`) và `data.attachments[].entry_id` |
| `created_time`                        | `data.posted_at`; không có thì lúc crawl (`flag_crawl_created_time = 0`)                                                                                                                                                                                              |
| `updated_time`                        | `data.updated_at` (thời gian thực tế: lần đẩy tin gần nhất)                                                                                                                                                                                                           |
| `author_id`, `author_name`            | `account_oid`, tên người bán                                                                                                                                                                                                                                          |
| `data.account_id`, `data.account_oid` | người bán                                                                                                                                                                                                                                                             |
| `data.attachments`                    | **mọi ảnh** của tin, mỗi ảnh 1 object; `entry_id = entry_rowkey`, `type = "photo"`, field không có dữ liệu = `""`                                                                                                                                                     |
| còn lại trong `data`                  | như mục 10.10 (không có `url`, `images`, `seller`: thông tin người bán ở topic người đăng)                                                                                                                                                                            |

#### `hydra-images-incoming`: ảnh bài đăng (1 message / ảnh, không gửi video)

```json
{
  "url": "https://cdn.chotot.com/...jpg",
  "media_url": "https://cdn.chotot.com/...jpg",
  "image_type": "photo",
  "updated_time": 1792656920589,
  "tenant_ids": ["9c70933aff6b2a6d08c687a6cbb6b765"],
  "post_id": "135401339",
  "created_time": 1792656917000,
  "entry_rowkey": "a7b22f86dde3dd42eb157bdd2fb1c2d2",
  "profile_id": "6aca663149b2732717abdd050062fe6e",
  "tenants": [
    {
      "id": "9c70933aff6b2a6d08c687a6cbb6b765",
      "name": "HY Global",
      "project": "global",
      "category_ids": []
    }
  ],
  "channel_name": "market"
}
```

`post_id`, `entry_rowkey`, `created_time`, `updated_time` theo bài đăng (mọi ảnh của 1 tin dùng chung).

#### `hydra-profile-bio-chotot_lss-h`: thông tin người đăng (1 message / người bán)

```json
{
  "doc_id": "b9f3fc8c7281753d9c0276e16ec33d31",
  "entry_rowkey": "b9f3fc8c7281753d9c0276e16ec33d31",
  "profile_id": "b9f3fc8c7281753d9c0276e16ec33d31",
  "about": "",
  "gender": "FEMALE",
  "channel_name": "market",
  "profile_type": "person",
  "url": "https://www.chotot.com/user/b9f3fc8c7281753d9c0276e16ec33d31",
  "name": "Trường Giang",
  "avatar": "https://cdn.chotot.com/uac2/13125056",
  "is_company": false,
  "phone": "0368931991",
  "banner": null,
  "address": "...",
  "joined_at": 1559975230000,
  "joined_duration": "7 năm 4 tháng",
  "profile_updated_at": 1792656529000,
  "is_active": true,
  "phone_verified": true,
  "email_verified": false,
  "followers": 0,
  "followings": 0,
  "rating": 0,
  "total_rating": 0,
  "total_rating_as_seller": 0,
  "total_rating_as_buyer": 0,
  "sold_ads": 0,
  "live_ads": 1,
  "response_rate": 0,
  "response_rate_text": "Thỉnh thoảng",
  "response_time": -1,
  "response_time_text": "Phản hồi chậm",
  "company_name": null,
  "company_logo": null
}
```

| Field                                  | Nguồn                                                                  |
| -------------------------------------- | ---------------------------------------------------------------------- |
| `doc_id`, `entry_rowkey`, `profile_id` | `account_oid`                                                          |
| `about`                                | `profile.description` trên trang người bán; không có → `""`            |
| `gender`                               | `profile.gender` (đổi về `"MALE"` / `"FEMALE"`); không có → `"FEMALE"` |
| `profile_type`, `channel_name`         | cố định `"person"`, `"market"`                                         |
| còn lại                                | các field người bán (bảng `seller` ở mục 10.10)                        |

Tin không có `account_oid` thì không gửi topic này.

#### `hydra-images-incoming-avatar`: ảnh người đăng (1 message / người bán có avatar)

```json
{
  "url": "https://cdn.chotot.com/uac2/13125056",
  "media_url": "https://cdn.chotot.com/uac2/13125056",
  "image_type": "avatar",
  "updated_time": 1793069487331,
  "tenant_ids": ["9c70933aff6b2a6d08c687a6cbb6b765"],
  "image_key": "chotot_avatar/b9f3fc8c7281753d9c0276e16ec33d31",
  "created_time": 1793069487331,
  "entry_rowkey": "b9f3fc8c7281753d9c0276e16ec33d31",
  "profile_id": "b9f3fc8c7281753d9c0276e16ec33d31",
  "channel_name": "market"
}
```

`created_time`, `updated_time` = thời điểm crawl. Chỉ gửi avatar (không gửi banner); người bán không có avatar thì không gửi.

### 10.10. Schema `data` (AdItem nội bộ)

Dữ liệu tin sau khi crawl, trước khi dựng 4 topic: phần tin vào `data` của topic bài đăng (bỏ `url`, `images`, `seller`), `seller` vào topic người đăng và avatar.

| Trường                                                                  | Ý nghĩa                                                                           |
| ----------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| `source`, `site`                                                        | `chotot`; site con: `chotot` / `xe` / `vieclamtot` / `nhatot`                     |
| `ad_id`, `internal_ad_id`                                               | `list_id` (ID trên URL), `ad_id` nội bộ của Chợ Tốt                               |
| `url`, `title`, `description`, `category`, `category_name`, `condition` |                                                                                   |
| `price`, `price_text`                                                   | giá dạng số, chuỗi hiển thị. Có thể `null`: việc làm, tin cho tặng, không ghi giá |
| `salary_min`, `salary_max`, `salary_type`                               | chỉ có ở việc làm                                                                 |
| `location`                                                              | Schema cố định, xem bảng `location` bên dưới                                      |
| `posted_at`, `updated_at`, `crawled_at`                                 | ms: đăng lần đầu, lần đẩy tin gần nhất, lúc crawl                                 |
| `images`, `videos`                                                      |                                                                                   |
| `seller`                                                                | Schema cố định, xem bảng `seller` bên dưới                                        |
| `specs`, `specs_labels`                                                 | thông số `{id: giá trị}` và `{id: nhãn tiếng Việt}`                               |

Số điện thoại đầy đủ không có trong `__NEXT_DATA__` (chỉ có dạng che `097567****`), nên `seller.phone` được lấy qua API phone (mục 10.5).

#### Schema cố định của `seller` và `location`

**Mọi tin, ở cả 4 site, đều có đủ các key dưới đây, đúng thứ tự này; không có giá trị thì `null`.** Phía sau (Kafka consumer, ES mapping) luôn đọc được key, không phải kiểm tra key có tồn tại hay không. Danh sách key khai báo duy nhất ở `chotot/schema.py` (`SELLER_FIELDS`, `LOCATION_FIELDS`) và được áp ở parser, pipeline `normalize` và lúc dựng message Kafka. Thêm field mới: thêm tên vào `schema.py`. Key không có trong schema vẫn được giữ (đặt cuối) và có log cảnh báo `Field seller.x chưa có trong schema`.

`specs`, `specs_labels` **không** theo schema cố định: là map "mã thông số → giá trị", mỗi loại tin một bộ (xe có `mileage`, nhà có `rooms`...).

**`seller`**

| Field                                                                                                                                         | Nguồn                                                                                                                   | Thường có ở |
| --------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- | ----------- |
| `account_id`, `account_oid`                                                                                                                   | trang tin `ad`                                                                                                          | mọi site    |
| `url`                                                                                                                                         | `https://www.chotot.com/user/<account_oid>`                                                                             | mọi site    |
| `name`, `avatar`                                                                                                                              | trang người bán (ưu tiên), trang tin (dự phòng)                                                                         | mọi site    |
| `is_company`                                                                                                                                  | trang tin `ad.company_ad`                                                                                               | mọi site    |
| `phone`                                                                                                                                       | API phone (mục 10.5)                                                                                                    | mọi site    |
| `banner`, `address`, `about`, `gender`, `joined_at`, `joined_duration`, `profile_updated_at`, `is_active`, `phone_verified`, `email_verified` | trang người bán (mục 10.6)                                                                                              | mọi site    |
| `followers`, `followings`                                                                                                                     | API số liệu người bán (mục 10.7)                                                                                        | mọi site    |
| `rating`, `total_rating`                                                                                                                      | API `ratings` (mục 10.7); dự phòng trang tin `ad.average_rating_for_seller` / `total_rating_for_seller`                 | mọi site    |
| `total_rating_as_seller`, `total_rating_as_buyer`                                                                                             | API `ratings` `total_review_as_seller` / `total_review_as_buyer` (mục 10.7)                                             | mọi site    |
| `response_rate`                                                                                                                               | API `chat/user/get` (mục 10.7): tỷ lệ phản hồi chat (%)                                                                 | mọi site    |
| `response_rate_text`, `response_time`, `response_time_text`                                                                                   | API `chat/user/get` (mục 10.7): mô tả tỷ lệ phản hồi, thời gian phản hồi (nguyên giá trị API), mô tả thời gian phản hồi | mọi site    |
| `sold_ads`                                                                                                                                    | API `solds_by` `paging.total`; dự phòng trang tin `ad.sold_ads`                                                         | mọi site    |
| `live_ads`                                                                                                                                    | API `theia` `total`; dự phòng trang tin `ad.seller_info.live_ads`                                                       | mọi site    |
| `company_name`, `company_logo`                                                                                                                | trang tin `ad.company_name` / `company_logo`                                                                            | vieclamtot  |

**`location`**

| Field                             | Nguồn (`ad`)                                   | Ghi chú                           |
| --------------------------------- | ---------------------------------------------- | --------------------------------- |
| `address`                         | `ad_params.address` / `detail_address`         |                                   |
| `city`, `city_v3`                 | `region_name`, `region_name_v3`                | `*_v3`: tên sau sáp nhập địa giới |
| `district`                        | `area_name`                                    |                                   |
| `ward`, `ward_v3`                 | `ward_name`, `ward_name_v3`                    |                                   |
| `street`                          | `street_name`                                  | thường chỉ nhatot                 |
| `region_id`, `area_id`, `ward_id` | `region_v2`/`region`, `area_v2`/`area`, `ward` |                                   |
| `lat`, `lng`                      | `latitude`, `longitude`                        |                                   |

---

## 11. Cấu trúc thư mục

```
scrapy_chotot/
├── .env.example                  mẫu cấu hình (copy thành .env)
├── docker-compose.yml            Redis local, Kafka local + Kafka UI (--profile kafka)
├── requirements.txt
├── scrapy.cfg
├── logs/                         file log mỗi lần chạy, file detail_*.json của luồng test (không commit)
├── scripts/
│   └── seed_profiles.py          đẩy 1 profile test vào topic input (KAFKA_MODE=broker hoặc KAFKA_LOCAL=true)
└── chotot/
    ├── settings.py               đọc cấu hình từ .env
    ├── items.py                  AdItem: schema chung (nội bộ)
    ├── hydra_topics.py           dựng message 4 topic Kafka + giá trị cố định (owner, tenant, channel...)
    ├── schema.py                 schema cố định của seller, location (đủ key, null nếu không có)
    ├── spiders/
    │   ├── base.py               HydraBaseSpider: đọc profile, ES, source_status, log HYDRA-MONITOR
    │   └── chotot_spider.py      trang danh sách → trang chi tiết
    ├── parsers/
    │   ├── __init__.py           chọn parser theo domain
    │   ├── base.py               phần chung đọc __NEXT_DATA__
    │   └── sites.py              ChototParser, XeParser, VieclamtotParser, NhatotParser
    ├── pipelines/                normalize → validation → redis_dedup (từng topic) → kafka_pipeline (4 topic)
    ├── utils/
    │   ├── kafka_sender.py       gửi Kafka: broker | http
    │   ├── kafka_producer.py     kết nối Kafka broker (SSL)
    │   ├── redis_client.py       kết nối Redis
    │   ├── phone_api.py          gọi API lấy số điện thoại đầy đủ, cache Redis
│   ├── detail_test.py        luồng test -a detail_url: ghi message 4 topic ra logs/detail_*.json
│   ├── page_cache.py         phần chung tải trang + cache Redis (dùng cho trang người bán)
│   ├── seller_profile.py     tải trang người bán (name, avatar, banner, address, joined_at…), cache Redis
│   ├── seller_stats.py       6 API số liệu người bán (followers, sold_ads, rating, response_rate…), cache Redis
    │   ├── hydra.py              hàm tiện ích Hydra (entry_rowkey, thời gian, profile test)
    │   └── normalizer.py         làm sạch chữ
    ├── kafka_logging.py          gửi log [HYDRA-MONITOR] lên Kafka
    ├── extensions.py             ghi log ra file, bảng tổng kết
    ├── middlewares.py            User-Agent ngẫu nhiên, proxy
    └── ssl/                      chứng chỉ Kafka (không commit)
```

##
