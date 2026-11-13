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

        -- 3. Thông tin giá bán & Khuyến mãi (Chuẩn hóa NUMERIC)
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