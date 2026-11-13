Dtaa

# 📚 TÀI LIỆU HƯỚNG DẪN TỰ XÂY DỰNG DỰ ÁN DBT (TIKI RETAIL DATA PIPELINE)

> **Mục tiêu của tài liệu:** Hướng dẫn chi tiết từng bước, giải thích cặn kẽ bản chất và cung cấp công thức để **chính bạn tự tay viết mã** chuyển đổi dữ liệu từ Open Food Facts cũ sang **Tiki Retail** mới bằng **dbt (data build tool)**.

---

## 📑 MỤC LỤC

1. [Tổng quan Kiến trúc dbt trong DataLink](#1-tổng-quan-kiến-trúc-dbt-trong-datalink)
2. [Cấu trúc Thư mục và Vai trò từng file](#2-cấu-trúc-thư-mục-và-vai-trò-từng-file)
3. [Bước 1: Khai báo cấu hình dự án (`dbt_project.yml`)](#3-bước-1-khai-báo-cấu-hình-dự-án-dbt_projectyml)
4. [Bước 2: Khai báo Dữ liệu nguồn (`_source.yml`)](#4-bước-2-khai-báo-dữ-liệu-nguồn-_sourceyml)
5. [Bước 3: Tự viết tầng Staging (`models/staging/`)](#5-bước-3-tự-viết-tầng-staging-modelsstaging)
6. [Bước 4: Tự thiết kế và viết tầng Marts (`models/marts/`)](#6-bước-4-tự-thiết-kế-và-viết-tầng-marts-modelsmarts)
7. [Bước 5: Viết Kiểm thử chất lượng dữ liệu (`tests`)](#7-bước-5-viết-kiểm-thử-chất-lượng-dữ-liệu-tests)
8. [Bước 6: Các câu lệnh thực thi và gỡ lỗi (CLI Commands)](#8-bước-6-các-câu-lệnh-thực-thi-và-gỡ-lỗi-cli-commands)
9. [Cẩm nang các lỗi thường gặp và cách tự khắc phục](#9-cẩm-nang-các-lỗi-thường-gặp-và-cách-tự-khắc-phục)

---

## 1. TỔNG QUAN KIẾN TRÚC DBT TRONG DATALINK

Dự án của bạn áp dụng mô hình **ELT & Medallion Architecture** (Kiến trúc huy chương), chia làm 3 tầng dữ liệu rõ rệt:

```
[ raw_data ]          ──► [ staging_data ]         ──► [ mart_data ]
(Bronze Layer)            (Silver Layer)               (Gold Layer)
Dữ liệu thô từ Tiki       Làm sạch, chuẩn hoá tên,     Mô hình Star Schema
Nạp bởi dlt pipeline      ép kiểu, lọc dữ liệu rác     Phục vụ Apache Superset
Materialized: TABLE       Materialized: VIEW           Materialized: TABLE
```

* **Tại sao Staging nên là `VIEW`?**
  Staging chỉ là bước chuẩn hoá dữ liệu trung gian, tạo `VIEW` giúp không tốn thêm dung lượng ổ cứng và luôn phản ánh dữ liệu mới nhất từ `raw_data`.
* **Tại sao Marts nên là `TABLE`?**
  Marts chứa các bảng Fact và Dimension đã được JOIN và tính toán phức tạp. Tạo `TABLE` vật lý giúp Superset truy vấn với tốc độ cực nhanh (vài mili-giây).

---

## 2. CẤU TRÚC THƯ MỤC VÀ VAI TRÒ TỪNG FILE

Trong thư mục `transform/`, bạn có cấu trúc như sau:

```text
transform/
├── dbt_project.yml              # File cấu hình trung tâm của toàn bộ dự án dbt
├── profiles.yml                 # Thông tin kết nối tới PostgreSQL (host, port, user, pass)
├── macros/
│   └── generate_schema_name.sql # Macro tuỳ biến tên schema (rất quan trọng)
└── models/
    ├── staging/                 # TẦNG 1: Làm sạch dữ liệu thô
    │   ├── _source.yml          # Định nghĩa bảng nguồn từ schema raw_data
    │   ├── _staging_models.yml  # Mô tả và viết test cho các model staging
    │   ├── stg_products.sql     # (Cần viết lại) Làm sạch bảng products
    │   ├── stg_sellers.sql      # (Cần tạo mới) Làm sạch bảng sellers
    │   ├── stg_categories.sql   # (Cần viết lại) Làm sạch bảng product_categories
    │   └── stg_reviews.sql      # (Cần tạo mới) Làm sạch bảng product_reviews
    └── marts/                   # TẦNG 2: Xây dựng Star Schema phân tích
        ├── _marts_models.yml    # Mô tả và viết test cho các model marts
        ├── dim_products.sql     # Bảng chiều: Thông tin danh mục sản phẩm, brand
        ├── dim_sellers.sql      # Bảng chiều: Thông tin nhà bán hàng, phân loại shop
        ├── fact_sales.sql       # Bảng sự thật: Giá, giảm giá, lượng bán, doanh thu
        └── fact_reviews.sql     # Bảng sự thật: Đánh giá, sentiment, điểm rating
```

---

## 3. BƯỚC 1: KHAI BÁO CẤU HÌNH DỰ ÁN (`dbt_project.yml`)

### Bản chất:

File này báo cho dbt biết:

1. Thư mục nào chứa models (`models-paths: ["models"]`).
2. Từng thư mục con sẽ được build thành loại gì (`view` hay `table`) và nằm ở **Schema nào trong PostgreSQL**.

### Nhiệm vụ của bạn:

Kiểm tra phần `models:` ở cuối file [dbt_project.yml](<file:///Users/trluat/Documents/Đồ%20án%20/DataLink/transform/dbt_project.yml>). Nếu bạn muốn bảng được sinh ra đúng schema `staging_data` và `mart_data` như file [init_db.sql](<file:///Users/trluat/Documents/Đồ%20án%20/DataLink/init_db.sql>) đã định nghĩa:

```yaml
models:
  datalink_transform:
    staging:
      +materialized: view
      +schema: staging_data     # Bảng view sẽ nằm ở schema staging_data
    marts:
      +materialized: table
      +schema: mart_data        # Bảng table sẽ nằm ở schema mart_data
```

> 💡 **Giải thích Macro `generate_schema_name.sql`:**
> Mặc định dbt sẽ tự cộng tiền tố (ví dụ: `public_staging_data`). Nhờ file [generate_schema_name.sql](<file:///Users/trluat/Documents/Đồ%20án%20/DataLink/transform/macros/generate_schema_name.sql>) có sẵn trong thư mục `macros/`, dbt sẽ lấy **chính xác** tên `staging_data` và `mart_data` mà bạn khai báo mà không bị thêm tiền tố thừa.

---

## 4. BƯỚC 2: KHAI BÁO DỮ LIỆU NGUỒN (`_source.yml`)

### Bản chất:

dbt không khuyến khích bạn viết cứng `FROM raw_data.products` trong câu SQL. Thay vào đó, dbt yêu cầu bạn khai báo trong `_source.yml` rồi dùng hàm `{{ source('tên_source', 'tên_bảng') }}`.

* **Lợi ích:** Tự động vẽ biểu đồ phả hệ dữ liệu (Data Lineage DAG), kiểm tra tính tươi mới của dữ liệu (freshness), và nếu mai mốt đổi tên schema thì chỉ cần sửa 1 chỗ duy nhất.

### Nhiệm vụ của bạn trong `models/staging/_source.yml`:

Xoá bỏ các bảng cũ của OpenFoodFacts và khai báo 4 bảng thô mà script `dlt` vừa nạp vào `raw_data`:

1. `products`: Chứa id, name, sku, brand_name, price, original_price, discount, discount_rate, quantity_sold, rating_average, review_count...
2. `sellers`: Chứa seller_id, seller_name, is_official_store...
3. `product_categories`: Chứa product_id, category_id, category_name...
4. `product_reviews`: Chứa review_id, product_id, customer_name, rating, title, content, created_at...

**Khung mẫu bạn cần điền:**

```yaml
version: 2

sources:
  - name: tiki_raw                        # Tên định danh nguồn (bạn tự đặt)
    description: "Dữ liệu bán lẻ thô thu thập từ Tiki API qua dlt pipeline"
    database: datalink_warehouse
    schema: raw_data
    tables:
      - name: products
        description: "Bảng sản phẩm thô"
      - name: sellers
        description: "Bảng nhà bán hàng thô"
      - name: product_categories
        description: "Bảng liên kết sản phẩm và danh mục ngành hàng"
      - name: product_reviews
        description: "Bảng nhận xét đánh giá thô từ người mua"
```

---

## 5. BƯỚC 3: TỰ VIẾT TẦNG STAGING (`models/staging/`)

### 5.1. Bản chất và Nguyên tắc vàng của Tầng Staging:

* **Mục đích duy nhất:** Biến dữ liệu thô (Bronze Layer) thành dữ liệu trung gian sạch (Silver Layer - 1:1 với nguồn), chuẩn hoá cấu trúc để các bảng Marts phân tích phía sau dùng chung một cách nhất quán.
* **Quy tắc 1:1:** Mỗi bảng trong tầng `raw_data` sẽ có chính xác 1 model staging tương ứng:
  * `raw_data.products` $\rightarrow$ `staging_data.stg_products`
  * `raw_data.sellers` $\rightarrow$ `staging_data.stg_sellers`
  * `raw_data.product_categories` $\rightarrow$ `staging_data.stg_product_categories`
  * `raw_data.product_reviews` $\rightarrow$ `staging_data.stg_product_reviews`
* **Các việc PHẢI LÀM ở Staging:**
  1. **Đổi tên cột (Aliasing):** Đưa về chuẩn `snake_case`, đặt tên mang tính định danh rõ ràng (ví dụ: `id` đổi thành `product_id`, `name` thành `product_name`).
  2. **Ép kiểu dữ liệu (Data Type Casting):** Chuyển đổi rõ ràng kiểu số (`BIGINT`, `NUMERIC`), kiểu logic (`BOOLEAN`), kiểu chuỗi (`VARCHAR`), kiểu thời gian (`TIMESTAMP`).
  3. **Làm sạch chuỗi (String Sanitization):** Dùng `TRIM()` để loại bỏ khoảng trắng thừa ở đầu/cuối chuỗi.
  4. **Xử lý giá trị rỗng (Null Handling):** Dùng `COALESCE()` để điền giá trị mặc định hợp lý (ví dụ: `COALESCE(brand_name, 'No Brand')`, `COALESCE(quantity_sold, 0)`).
  5. **Lọc dữ liệu rác (Filtering):** Loại bỏ bản ghi thiếu khoá chính hoặc dữ liệu vô nghĩa (`WHERE id IS NOT NULL`).
  6. **Giữ lại trường Metadata:** Giữ lại `_dlt_load_id` (đổi tên thành `dlt_load_id`) để phục vụ việc truy vết luồng dữ liệu (Data Lineage / Data Auditing).
* **Điều CẤM KỴ ở Staging:**
  * ❌ **Không JOIN phức tạp:** Không JOIN nhiều bảng thô lại với nhau ở đây (JOIN chỉ làm ở tầng Marts).
  * ❌ **Không gom nhóm (GROUP BY) hay tổng hợp (AGGREGATION):** Dữ liệu staging phải giữ nguyên độ chi tiết (grain) của nguồn.

---

### 5.2. Cấu trúc chuẩn 3 khối CTE (Common Table Expression):

Mọi file staging đều áp dụng thống nhất mô hình 3 phần:

```sql
-- KHỐI 1: Trích xuất dữ liệu gốc qua Jinja source()
WITH source AS (
    SELECT * FROM {{ source('apitiki_raw', 'tên_bảng_nguồn') }}
),

-- KHỐI 2: Làm sạch, đổi tên, ép kiểu dữ liệu
cleaned AS (
    SELECT
        ...
    FROM source
    WHERE ... -- Lọc bản ghi rác
)

-- KHỐI 3: Trả về kết quả cuối
SELECT * FROM cleaned
```

---

### 5.3. Chi tiết mã nguồn và Giải thích từng bảng Staging:

#### 1. Bảng Sản phẩm: `models/staging/stg_products.sql`

* **Mục đích:** Là bảng trung tâm làm sạch toàn bộ danh mục sản phẩm từ Tiki, chuẩn hóa giá trị tiền tệ, lượt bán, thương hiệu và thông tin danh mục gốc.
* **Mã SQL hoàn chỉnh:**

```sql
-- models/staging/stg_products.sql
{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('apitiki_raw', 'products') }}
),

cleaned AS (
    SELECT
        -- 1. Khóa chính & Định danh sản phẩm
        CAST(id AS BIGINT)                               AS product_id,
        TRIM(sku)                                        AS sku,
        TRIM(name)                                       AS product_name,

        -- 2. Thương hiệu (Nếu null thì gán 'No Brand')
        CAST(brand_id AS BIGINT)                         AS brand_id,
        COALESCE(TRIM(brand_name), 'No Brand')           AS brand_name,

        -- 3. Thông tin giá bán & Khuyến mãi (Chuẩn hóa kiểu số thực NUMERIC)
        CAST(price AS NUMERIC(15, 2))                    AS price,
        CAST(original_price AS NUMERIC(15, 2))           AS original_price,
        CAST(discount AS NUMERIC(15, 2))                 AS discount_amount,
        CAST(discount_rate AS INTEGER)                   AS discount_rate_percent,

        -- 4. Đo lường lượt bán & Đánh giá khách hàng
        COALESCE(quantity_sold, 0)::BIGINT               AS quantity_sold,
        TRIM(quantity_sold_text)                         AS quantity_sold_display,
        ROUND(CAST(rating_average AS NUMERIC), 2)        AS rating_average,
        COALESCE(review_count, 0)::INTEGER               AS review_count,

        -- 5. Thông tin nhà bán hàng & Xuất xứ
        CAST(seller_id AS BIGINT)                        AS seller_id,
        TRIM(seller_name)                                AS seller_name,
        COALESCE(is_official_store, FALSE)               AS is_official_store,
        TRIM(origin)                                     AS origin_country,

        -- 6. Danh mục chính của sản phẩm
        CAST(primary_category_id AS BIGINT)              AS category_id,
        TRIM(primary_category_name)                      AS category_name,

        -- 7. Thông tin bổ sung, hình ảnh & đường dẫn
        fastest_delivery_time                            AS fastest_delivery_time,
        thumbnail_url                                    AS thumbnail_url,
        url_path                                         AS product_url,

        -- 8. Metadata hệ thống nạp dữ liệu (Audit Trail)
        _dlt_load_id                                     AS dlt_load_id

    FROM source
    WHERE id IS NOT NULL
      AND name IS NOT NULL
      AND LENGTH(TRIM(name)) > 0
)

SELECT * FROM cleaned
```

* **Giải thích chi tiết các nghiệp vụ xử lý:**
  * `id` $\rightarrow$ `product_id`: Ép về `BIGINT` vì ID trên sàn Tiki rất lớn (có thể vượt giới hạn `INTEGER` 2 tỷ).
  * `COALESCE(TRIM(brand_name), 'No Brand')`: Nhiều sản phẩm không có nhãn hiệu (ví dụ ốp lưng, đồ gia dụng no-name). Thay vì để `NULL` gây khó vẽ chart trên Superset, ta gán về nhãn chuẩn `'No Brand'`.
  * `price`, `original_price`, `discount_amount`: Ép về kiểu `NUMERIC(15, 2)` để đảm bảo độ chính xác tuyệt đối trong tính toán tài chính, tránh sai số dấu phẩy động của kiểu `FLOAT`.
  * `COALESCE(quantity_sold, 0)`: Với các sản phẩm mới ra mắt chưa có lượt mua nào, API trả về `null`. Cần đưa về `0` để khi SUM doanh thu không làm triệt tiêu các phép cộng (trong SQL: `NULL + 1 = NULL`).
  * `ROUND(CAST(rating_average AS NUMERIC), 2)`: Làm tròn điểm đánh giá về đúng 2 chữ số thập phân (ví dụ: `4.67` sao).
  * `WHERE id IS NOT NULL AND LENGTH(TRIM(name)) > 0`: Loại bỏ triệt để các record rác không có mã hoặc có tên trắng rỗng.

---

#### 2. Bảng Nhà bán hàng: `models/staging/stg_sellers.sql`

* **Mục đích:** Chuẩn hóa thực thể Nhà bán (Merchant / Seller) hoạt động trên sàn Tiki, tách bạch cờ gian hàng chính hãng.
* **Mã SQL hoàn chỉnh:**

```sql
-- models/staging/stg_sellers.sql
{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('apitiki_raw', 'sellers') }}
),

cleaned AS (
    SELECT
        -- 1. Khóa chính định danh nhà bán
        CAST(seller_id AS BIGINT)                               AS seller_id,

        -- 2. Tên gian hàng (Xử lý khoảng trắng và giá trị rỗng)
        COALESCE(TRIM(seller_name), 'Nhà bán chưa đặt tên')     AS seller_name,

        -- 3. Cờ xác định gian hàng chính hãng (Tiki Trading / Official Store)
        COALESCE(is_official_store, FALSE)                      AS is_official_store,

        -- 4. Metadata kiểm toán
        _dlt_load_id                                            AS dlt_load_id,

        -- 5. Khử trùng lặp bản ghi nếu cùng một seller xuất hiện nhiều lần
        ROW_NUMBER() OVER (
            PARTITION BY seller_id 
            ORDER BY _dlt_load_id DESC
        ) AS row_num

    FROM source
    WHERE seller_id IS NOT NULL
)

-- Chỉ lấy 1 bản ghi mới nhất cho mỗi seller_id
SELECT
    seller_id,
    seller_name,
    is_official_store,
    dlt_load_id
FROM cleaned
WHERE row_num = 1
```

* **Giải thích chi tiết các nghiệp vụ xử lý:**
  * `ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY _dlt_load_id DESC)`: Khi script Ingest chạy qua nhiều trang sản phẩm, cùng một seller (ví dụ Tiki Trading) có thể được ghi nhận lặp lại. Đoạn code này giữ lại đúng **1 dòng mới nhất** cho từng `seller_id`, ngăn chặn lỗi trùng lặp khoá chính (`unique test failure`).
  * `COALESCE(is_official_store, FALSE)`: Đảm bảo trường này chỉ nhận giá trị nhị phân `TRUE` hoặc `FALSE`, không bị `NULL`.

---

#### 3. Bảng Phân loại Danh mục: `models/staging/stg_product_categories.sql`

* **Mục đích:** Bảng cầu nối (Bridge/Mapping table) thể hiện mối quan hệ giữa một sản phẩm với các ngành hàng/danh mục tương ứng.
* **Mã SQL hoàn chỉnh:**

```sql
-- models/staging/stg_product_categories.sql
{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('apitiki_raw', 'product_categories') }}
),

cleaned AS (
    SELECT
        -- 1. Cặp khóa liên kết Sản phẩm - Ngành hàng
        CAST(product_id AS BIGINT)                              AS product_id,
        CAST(category_id AS BIGINT)                             AS category_id,

        -- 2. Tên danh mục ngành hàng tiếng Việt
        TRIM(category_name)                                     AS category_name,

        -- 3. Metadata kiểm toán
        _dlt_load_id                                            AS dlt_load_id,

        -- 4. Đánh số khử trùng lặp cặp (product_id, category_id)
        ROW_NUMBER() OVER (
            PARTITION BY product_id, category_id 
            ORDER BY _dlt_load_id DESC
        ) AS row_num

    FROM source
    WHERE product_id IS NOT NULL
      AND category_id IS NOT NULL
)

SELECT
    product_id,
    category_id,
    category_name,
    dlt_load_id
FROM cleaned
WHERE row_num = 1
```

* **Giải thích chi tiết các nghiệp vụ xử lý:**
  * `WHERE product_id IS NOT NULL AND category_id IS NOT NULL`: Loại bỏ các cặp liên kết mồ côi (thiếu ID sản phẩm hoặc thiếu ID danh mục).
  * `PARTITION BY product_id, category_id`: Bảo đảm mỗi sản phẩm chỉ thuộc về một danh mục đúng một lần, tránh nhân đôi dữ liệu khi JOIN ở tầng Marts.

---

#### 4. Bảng Đánh giá người mua: `models/staging/stg_product_reviews.sql`

* **Mục đích:** Chuẩn hóa các phản hồi, nhận xét, số sao đánh giá từ khách hàng thực tế mua hàng trên Tiki.
* **Mã SQL hoàn chỉnh:**

```sql
-- models/staging/stg_product_reviews.sql
{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('apitiki_raw', 'product_reviews') }}
),

cleaned AS (
    SELECT
        -- 1. Khóa chính đánh giá & Khóa ngoại sản phẩm
        CAST(review_id AS BIGINT)                               AS review_id,
        CAST(product_id AS BIGINT)                              AS product_id,

        -- 2. Người đánh giá (Ẩn danh nếu null)
        COALESCE(TRIM(customer_name), 'Khách hàng ẩn danh')     AS customer_name,

        -- 3. Thang điểm đánh giá (Từ 1 đến 5 sao)
        CAST(rating AS INTEGER)                                 AS rating,

        -- 4. Nội dung nhận xét
        TRIM(title)                                             AS review_title,
        TRIM(content)                                           AS review_content,

        -- 5. Thời gian đánh giá: Chuyển đổi Unix Epoch Timestamp (giây) sang TIMESTAMP chuẩn
        TO_TIMESTAMP(CAST(created_at AS BIGINT))                AS reviewed_at,

        -- 6. Metadata kiểm toán
        _dlt_load_id                                            AS dlt_load_id,

        -- 7. Khử trùng lặp mã đánh giá
        ROW_NUMBER() OVER (
            PARTITION BY review_id 
            ORDER BY _dlt_load_id DESC
        ) AS row_num

    FROM source
    WHERE review_id IS NOT NULL
      AND product_id IS NOT NULL
      AND rating IS NOT NULL
)

SELECT
    review_id,
    product_id,
    customer_name,
    rating,
    review_title,
    review_content,
    reviewed_at,
    dlt_load_id
FROM cleaned
WHERE row_num = 1
```

* **Giải thích chi tiết các nghiệp vụ xử lý:**
  * `TO_TIMESTAMP(CAST(created_at AS BIGINT)) AS reviewed_at`: API của Tiki trả về thời gian viết review dưới dạng số nguyên Unix epoch tính bằng giây (ví dụ: `1710500000`). Hàm `TO_TIMESTAMP()` chuyển đổi con số vô nghĩa này thành kiểu ngày giờ chuẩn của PostgreSQL (`2024-03-15 10:53:20`), cho phép vẽ biểu đồ xu hướng theo ngày/tháng/năm trên Superset.
  * `COALESCE(TRIM(customer_name), 'Khách hàng ẩn danh')`: Người mua thường không đặt tên công khai hoặc để trống. Việc gán giá trị mặc định giúp báo cáo phân tích khách hàng nhìn chuyên nghiệp và sạch đẹp.
  * Lọc `WHERE rating IS NOT NULL`: Đảm bảo mọi review đều có số điểm đánh giá hợp lệ.

---

### 5.4. Khai báo Kiểm thử Chất lượng Dữ liệu Staging (`_staging_models.yml`):

Sau khi viết 4 file SQL, bạn cần tạo file `models/staging/_staging_models.yml` để dbt tự động kiểm tra chất lượng dữ liệu:

```yaml
version: 2

models:
  - name: stg_products
    description: "Bảng staging dữ liệu sản phẩm Tiki đã chuẩn hóa"
    columns:
      - name: product_id
        description: "Khóa chính sản phẩm"
        tests:
          - unique
          - not_null
      - name: product_name
        description: "Tên sản phẩm"
        tests:
          - not_null
      - name: price
        description: "Giá bán hiện tại"
        tests:
          - not_null

  - name: stg_sellers
    description: "Bảng staging nhà bán hàng Tiki"
    columns:
      - name: seller_id
        description: "Khóa chính nhà bán hàng"
        tests:
          - unique
          - not_null

  - name: stg_product_categories
    description: "Bảng liên kết sản phẩm và danh mục"
    columns:
      - name: product_id
        tests:
          - not_null
      - name: category_id
        tests:
          - not_null

  - name: stg_product_reviews
    description: "Bảng nhận xét đánh giá sản phẩm"
    columns:
      - name: review_id
        tests:
          - unique
          - not_null
      - name: rating
        description: "Số sao đánh giá (chỉ chấp nhận từ 1 đến 5)"
        tests:
          - not_null
          - accepted_values:
              values: [1, 2, 3, 4, 5]
      - name: product_id
        description: "Khóa ngoại tham chiếu đến bảng sản phẩm"
        tests:
          - not_null
          - relationships:
              to: ref('stg_products')
              field: product_id
```

---

### 5.5. Lệnh thực thi kiểm tra Tầng Staging:

Chạy các lệnh sau trong terminal để dbt build toàn bộ 4 view staging vào schema `staging_data`:

```bash
# 1. Chạy biên dịch và tạo các View Staging:
$DBT run --select staging

# 2. Chạy toàn bộ bài test chất lượng dữ liệu:
$DBT test --select staging
```

Khi màn hình hiển thị toàn bộ chữ xanh **`PASS`**, tầng Staging của bạn đã hoàn hảo và sẵn sàng phục vụ cho Tầng Marts!

---

## 6. BƯỚC 4: TỰ THIẾT KẾ VÀ VIẾT TẦNG MARTS (`models/marts/`)

### Bản chất của Tầng Marts:

* Đọc dữ liệu từ các model Staging thông qua hàm `{{ ref('tên_model_staging') }}` (Ví dụ: `{{ ref('stg_products') }}`).
* **Đây là nơi diễn ra toàn bộ phép tính toán kinh doanh** mà trước đây script Ingestion đã bỏ qua.
* Xây dựng theo mô hình **Star Schema** gồm:
  * **Dimension Tables (Bảng Chiều - `dim_`):** Chứa thông tin mô tả ngữ cảnh (Ai, Cái gì, Ở đâu).
  * **Fact Tables (Bảng Sự thật - `fact_`):** Chứa các con số đo lường, chỉ số hiệu suất KPI (Doanh thu, số lượng, tiền bạc).

### Các Model Marts bạn nên xây dựng:

#### 1. Bảng chiều sản phẩm: `models/marts/dim_products.sql`

* Đọc từ `{{ ref('stg_products') }}`.
* Bổ sung phân nhóm kinh doanh (Business Logic):
  * Dùng `CASE WHEN` để phân loại phân khúc giá (`price_tier`):
    * Giá < 200.000 $\rightarrow$ `'Budget'`
    * Giá từ 200.000 đến 2.000.000 $\rightarrow$ `'Mid-Range'`
    * Giá > 2.000.000 $\rightarrow$ `'Premium'`
  * Phân loại mức độ giảm giá (`discount_band`):
    * `0%` $\rightarrow$ `'Không giảm giá'`
    * `1% - 20%` $\rightarrow$ `'Giảm nhẹ'`
    * `> 20%` $\rightarrow$ `'Giảm sâu'`

#### 2. Bảng chiều nhà bán hàng: `models/marts/dim_sellers.sql`

* Đọc từ `{{ ref('stg_sellers') }}`.
* Bổ sung phân loại loại hình gian hàng:
  * `CASE WHEN is_official_store THEN 'Gian Hàng Chính Hãng (Mall)' ELSE 'Shop Thông Thường' END AS seller_type`.

#### 3. Bảng sự thật bán hàng: `models/marts/fact_product_performance.sql`

* JOIN giữa `stg_products` và `stg_sellers` qua `seller_id`.
* **Tính toán các chỉ số kinh doanh cốt lõi (KPIs)**:
  * **Doanh thu ước tính:** `(price * quantity_sold) AS estimated_revenue`.
  * **Số tiền tiết kiệm cho khách:** `(original_price - price) AS discount_savings`.
  * **Độ hấp dẫn sản phẩm (Rating Score):** Tích hợp giữa số lượng review và điểm đánh giá trung bình.

#### 4. Bảng sự thật đánh giá khách hàng: `models/marts/fact_customer_feedback.sql`

* Đọc từ `{{ ref('stg_product_reviews') }}` JOIN với `{{ ref('stg_products') }}` qua `product_id`.
* Phân loại tâm lý khách hàng (Customer Sentiment):
  * `rating >= 4` $\rightarrow$ `'Tích cực (Khen)'`
  * `rating = 3` $\rightarrow$ `'Trung lập'`
  * `rating <= 2` $\rightarrow$ `'Tiêu cực (Khiếu nại/Chê)'`

---

## 7. BƯỚC 5: VIẾT KIỂM THỬ CHẤT LƯỢNG DỮ LIỆU (`tests`)

### Bản chất:

Một Data Engineer giỏi không chỉ viết code chuyển đổi mà còn phải **chứng minh dữ liệu không bị lỗi**. dbt hỗ trợ viết test bằng file `.yml`.

### Nhiệm vụ:

Tạo hoặc cập nhật file `models/staging/_staging_models.yml`:

* **`unique`**: Đảm bảo ID không bị trùng lặp.
* **`not_null`**: Đảm bảo không bị thiếu khoá chính hoặc tên.
* **`accepted_values`**: Đảm bảo cột chỉ nhận các giá trị hợp lệ (Ví dụ: `rating` chỉ được nằm trong `[1, 2, 3, 4, 5]`).
* **`relationships`**: Đảm bảo tính toàn vẹn tham chiếu (Ví dụ: `product_id` trong bảng reviews phải tồn tại trong bảng products).

**Mẫu kiểm thử bạn có thể viết:**

```yaml
version: 2

models:
  - name: stg_products
    columns:
      - name: product_id
        tests:
          - unique
          - not_null
      - name: product_name
        tests:
          - not_null

  - name: stg_product_reviews
    columns:
      - name: rating
        tests:
          - accepted_values:
              values: [1, 2, 3, 4, 5]
      - name: product_id
        tests:
          - relationships:
              to: ref('stg_products')
              field: product_id
```

---

## 8. BƯỚC 6: CÁC CÂU LỆNH THỰC THI VÀ GỠ LỖI (CLI COMMANDS)

Mở Terminal trên máy Mac của bạn, di chuyển vào thư mục `transform` và kích hoạt môi trường ảo (venv):

```bash
# 1. Di chuyển vào thư mục transform
cd "/Users/trluat/Documents/Đồ án /DataLink/transform"

# 2. Sử dụng đúng file thực thi dbt trong venv (tránh lỗi No such file or directory)
DBT="/Users/trluat/Documents/Đồ án /DataLink/ingestion/venv/bin/dbt"
```

### Các lệnh bạn sẽ dùng liên tục:

1. **Kiểm tra kết nối Database:**

   ```bash
   $DBT debug
   ```

   *(Nếu thấy dòng `All checks passed!` màu xanh là kết nối thành công 100%).*
2. **Chạy thử biên dịch SQL (không ghi vào DB, chỉ kiểm tra cú pháp Jinja/SQL):**

   ```bash
   $DBT compile
   ```
3. **Chạy thực thi tạo các bảng và view vào PostgreSQL:**

   ```bash
   # Chạy toàn bộ các model:
   $DBT run

   # Hoặc chỉ chạy riêng 1 model đang viết dở:
   $DBT run --select stg_products

   # Hoặc chỉ chạy toàn bộ thư mục staging:
   $DBT run --select staging
   ```
4. **Chạy kiểm thử chất lượng dữ liệu:**

   ```bash
   $DBT test
   ```
5. **Tự động sinh tài liệu và biểu đồ Lineage DAG:**

   ```bash
   $DBT docs generate
   $DBT docs serve --port 8081
   ```

   *(Truy cập `http://localhost:8081` để thấy toàn bộ sơ đồ luồng dữ liệu cực kỳ đẹp mắt).*

---

## 9. CẨM NANG CÁC LỖI THƯỜNG GẶP VÀ CÁCH TỰ KHẮC PHỤC

| Hiện tượng lỗi                                                                                   | Nguyên nhân                                                         | Cách bạn tự sửa                                                                                                                      |
| :--------------------------------------------------------------------------------------------------- | :-------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------- |
| `relation "raw_data.xxx" does not exist`                                                           | Bạn gọi `source` trước khi script `dlt` chạy nạp dữ liệu. | Chạy lại nút "Chạy Pipeline" trên Web hoặc chạy `python3 ingest_openfoodfact.py` để nạp dữ liệu vào `raw_data` trước. |
| `Compilation Error: Model 'xxx' depends on a source named 'openfoodfacts_raw' which was not found` | Bạn vẫn còn file model cũ gọi tên source cũ.                   | Mở model đó ra và đổi tên source thành tên mới bạn khai báo trong `_source.yml`.                                           |
| `schema "staging_data" does not exist`                                                             | Database PostgreSQL chưa tạo schema này.                           | Chạy `CREATE SCHEMA IF NOT EXISTS staging_data;` trong DBeaver/SQL Lab.                                                               |
| `Syntax error at or near ","`                                                                      | Dư hoặc thiếu dấu phẩy trong khối `SELECT`.                   | Kiểm tra dòng cuối cùng trong mệnh đề `SELECT` xem có bị dư dấu `,` trước lệnh `FROM` không.                        |

---

> 🎯 **Lời khuyên:** Hãy bắt đầu từ **Bước 1 (`dbt_project.yml`)** $\rightarrow$ **Bước 2 (`_source.yml`)** $\rightarrow$ viết **1 file Staging đầu tiên (`stg_products.sql`)** rồi gõ `$DBT run --select stg_products` để tận hưởng cảm giác dòng code đầu tiên được build thành công vào PostgreSQL nhé!
