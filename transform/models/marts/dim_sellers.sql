-- models/marts/dim_sellers.sql
-- Bảng chiều nhà bán hàng: Phân loại gian hàng và tổng hợp quy mô hoạt động
{{ config(materialized='table') }}

WITH sellers AS (
    SELECT * FROM {{ ref('stg_sellers') }}
),

product_stats AS (
    SELECT
        seller_id,
        COUNT(product_id)                                  AS total_products_listed,
        SUM(quantity_sold)                                 AS total_items_sold,
        SUM(price * quantity_sold)                         AS total_estimated_revenue,
        ROUND(AVG(rating_average), 2)                      AS avg_store_rating
    FROM {{ ref('stg_products') }}
    WHERE seller_id IS NOT NULL
    GROUP BY seller_id
)

SELECT
    s.seller_id,
    s.seller_name,
    s.is_official_store,

    -- Phân loại hình thức gian hàng
    CASE
        WHEN s.is_official_store THEN 'Gian Hàng Chính Hãng (Mall)'
        ELSE 'Shop Thông Thường'
    END AS seller_type,

    COALESCE(p.total_products_listed, 0)                   AS total_products_listed,
    COALESCE(p.total_items_sold, 0)                        AS total_items_sold,
    COALESCE(p.total_estimated_revenue, 0)::NUMERIC(18, 2) AS total_estimated_revenue,
    COALESCE(p.avg_store_rating, 0.0)                      AS avg_store_rating,
    s.dlt_load_id

FROM sellers s
LEFT JOIN product_stats p ON s.seller_id = p.seller_id
