-- models/marts/fact_reviews.sql
-- Bảng sự thật đo lường chất lượng dịch vụ và phân tích phản hồi của người tiêu dùng
{{ config(materialized='table') }}

WITH reviews AS (
    SELECT * FROM {{ ref('stg_product_reviews') }}
),

products AS (
    SELECT
        product_id,
        product_name,
        category_id,
        category_name,
        brand_name,
        seller_id,
        price
    FROM {{ ref('stg_products') }}
)

SELECT
    -- Khóa chính & Khóa ngoại
    r.review_id,
    r.product_id,
    p.category_id,
    p.seller_id,

    -- Thuộc tính ngữ cảnh hỗ trợ Slice & Dice trên Dashboard
    p.product_name,
    p.brand_name,
    p.category_name,
    p.price                                                         AS product_price,
    r.customer_name,

    -- Số sao và phân loại cảm xúc khách hàng (Customer Sentiment)
    r.rating,
    CASE
        WHEN r.rating >= 4 THEN 'Tích cực (Khen)'
        WHEN r.rating = 3  THEN 'Trung lập'
        ELSE 'Tiêu cực (Khiếu nại/Chê)'
    END AS sentiment_category,

    -- Tiêu đề và nội dung nhận xét
    r.review_title,
    r.review_content,

    -- Cờ kiểm tra khách hàng có để lại nhận xét chi tiết không
    CASE
        WHEN r.review_content IS NOT NULL AND LENGTH(TRIM(r.review_content)) > 0 THEN TRUE
        ELSE FALSE
    END AS has_written_feedback,

    -- Thời gian đánh giá (chuẩn timestamp phục vụ biểu đồ xu hướng)
    r.reviewed_at,
    DATE(r.reviewed_at)                                             AS review_date,
    r.dlt_load_id

FROM reviews r
LEFT JOIN products p ON r.product_id = p.product_id
