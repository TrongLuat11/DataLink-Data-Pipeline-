-- models/marts/fact_flash_sales.sql
-- Bảng sự thật đo lường hiệu suất các chiến dịch Flash Sale & Deal Giảm Giá Sốc
{{ config(materialized='table') }}

WITH flash_sales AS (
    SELECT * FROM {{ ref('stg_flash_sales') }}
),

products AS (
    SELECT
        product_id,
        category_id,
        category_name,
        brand_name,
        seller_id,
        is_official_store
    FROM {{ ref('stg_products') }}
)

SELECT
    -- Khóa chính & Khóa ngoại chiều
    f.deal_id,
    f.product_id,
    p.category_id,
    p.seller_id,

    -- Thuộc tính phân loại hỗ trợ Slice & Dice trên Dashboard
    f.product_name,
    p.brand_name,
    p.category_name,
    COALESCE(p.is_official_store, FALSE)                        AS is_official_store,

    -- Khung giờ & Trạng thái đợt bán
    f.slot_time,
    f.deal_status,

    -- Thông tin giá & Mức giảm giá
    f.special_price,
    f.original_price,
    f.discount_amount,
    f.discount_percent,

    -- Tiến độ bán ra (Quota & Units Sold)
    f.progress_qty                                              AS deal_quantity_sold,
    f.progress_max                                              AS deal_quota_total,

    -- Tỷ lệ cháy hàng (Sell-Out Rate %)
    ROUND(
        (f.progress_qty::NUMERIC / NULLIF(f.progress_max, 0)) * 100, 2
    )                                                           AS sell_out_rate_percent,

    -- Phân loại mức độ thành công của Deal
    CASE
        WHEN (f.progress_qty::NUMERIC / NULLIF(f.progress_max, 0)) >= 0.80 THEN 'Cháy hàng (Hot Deal 🔥)'
        WHEN (f.progress_qty::NUMERIC / NULLIF(f.progress_max, 0)) >= 0.50 THEN 'Bán chạy (Fast Selling ⚡)'
        ELSE 'Bình thường (Normal)'
    END AS deal_performance_tier,

    -- Doanh thu tạo ra từ khung giờ Flash Sale (Deal GMV)
    (f.special_price * f.progress_qty)::NUMERIC(18, 2)          AS deal_estimated_revenue,

    -- Tổng số tiền giảm giá ưu đãi cho người mua
    (f.discount_amount * f.progress_qty)::NUMERIC(18, 2)        AS deal_savings_total,

    f.dlt_load_id

FROM flash_sales f
LEFT JOIN products p ON f.product_id = p.product_id
