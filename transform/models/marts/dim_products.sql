-- models/marts/dim_products.sql
-- Bảng chiều sản phẩm: Chuẩn hóa ngữ cảnh mô tả và phân khúc sản phẩm cho BI Dashboard
{{ config(materialized='table') }}

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
)

SELECT
    product_id,
    sku,
    product_name,
    brand_id,
    brand_name,
    category_id,
    category_name,
    seller_id,
    origin_country,
    is_official_store,
    price,
    original_price,
    discount_amount,
    discount_rate_percent,

    -- Phân khúc giá sản phẩm (Price Tiers)
    CASE
        WHEN price < 200000 THEN 'Phổ thông (< 200k)'
        WHEN price BETWEEN 200000 AND 2000000 THEN 'Tầm trung (200k - 2Tr)'
        ELSE 'Cao cấp (> 2Tr)'
    END AS price_tier,

    -- Phân loại mức độ giảm giá (Discount Bands)
    CASE
        WHEN discount_rate_percent = 0 OR discount_rate_percent IS NULL THEN 'Không giảm giá'
        WHEN discount_rate_percent BETWEEN 1 AND 20 THEN 'Giảm nhẹ (1 - 20%)'
        WHEN discount_rate_percent BETWEEN 21 AND 50 THEN 'Giảm sâu (21 - 50%)'
        ELSE 'Siêu giảm giá (> 50%)'
    END AS discount_band,

    -- Đánh giá sao phân nhóm (Rating Tiers)
    CASE
        WHEN rating_average >= 4.5 THEN 'Xuất sắc (>= 4.5★)'
        WHEN rating_average >= 4.0 THEN 'Tốt (4.0 - 4.4★)'
        WHEN rating_average >= 3.0 THEN 'Trung bình (3.0 - 3.9★)'
        WHEN rating_average > 0 THEN 'Kém (< 3.0★)'
        ELSE 'Chưa có đánh giá'
    END AS rating_tier,

    rating_average,
    review_count,
    quantity_sold,
    fastest_delivery_time,
    thumbnail_url,
    product_url,
    dlt_load_id

FROM products
