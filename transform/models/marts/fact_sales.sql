-- models/marts/fact_sales.sql
-- Bảng sự thật đo lường hiệu suất bán lẻ, doanh thu ước tính và giá trị chiết khấu
{{ config(materialized='table') }}

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

sellers AS (
    SELECT * FROM {{ ref('stg_sellers') }}
)

SELECT
    -- Cặp khóa chiều (Foreign Keys)
    p.product_id,
    p.seller_id,
    p.category_id,
    p.brand_id,

    -- Thuộc tính phân loại hỗ trợ lọc nhanh
    p.product_name,
    p.brand_name,
    p.category_name,
    s.seller_name,
    COALESCE(s.is_official_store, FALSE)                           AS is_official_store,

    -- Phân khúc giá sản phẩm (Price Tiers) phục vụ phân tích Slice & Dice
    CASE
        WHEN p.price < 200000 THEN 'Phổ thông (< 200k)'
        WHEN p.price BETWEEN 200000 AND 2000000 THEN 'Tầm trung (200k - 2Tr)'
        ELSE 'Cao cấp (> 2Tr)'
    END AS price_tier,

    -- Phân loại mức độ giảm giá (Discount Bands)
    CASE
        WHEN p.discount_rate_percent = 0 OR p.discount_rate_percent IS NULL THEN 'Không giảm giá'
        WHEN p.discount_rate_percent BETWEEN 1 AND 20 THEN 'Giảm nhẹ (1 - 20%)'
        WHEN p.discount_rate_percent BETWEEN 21 AND 50 THEN 'Giảm sâu (21 - 50%)'
        ELSE 'Siêu giảm giá (> 50%)'
    END AS discount_band,

    -- Các chỉ số tài chính và đo lường (Facts & Metrics)
    p.price,
    p.original_price,
    p.discount_amount,
    p.discount_rate_percent,
    p.quantity_sold,

    -- Doanh thu ước tính (Estimated Revenue = Price * Quantity Sold)
    (p.price * p.quantity_sold)::NUMERIC(18, 2)                    AS estimated_revenue,

    -- Tổng số tiền giảm giá tiết kiệm cho khách hàng (Discount Savings)
    (p.discount_amount * p.quantity_sold)::NUMERIC(18, 2)          AS discount_savings_total,

    -- Tổng giá trị hàng hóa theo giá niêm yết gốc (Original GMV)
    (p.original_price * p.quantity_sold)::NUMERIC(18, 2)           AS original_gross_merchandise_value,

    p.rating_average,
    p.review_count,
    p.dlt_load_id

FROM products p
LEFT JOIN sellers s ON p.seller_id = s.seller_id
