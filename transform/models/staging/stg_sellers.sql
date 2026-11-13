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
