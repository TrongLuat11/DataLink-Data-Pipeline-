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

        -- 5. Thời gian đánh giá: Chuyển đổi Unix Epoch Timestamp (giây) sang TIMESTAMP
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
