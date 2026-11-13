-- models/staging/stg_flash_sales.sql
{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('apitiki_raw', 'flash_sales') }}
),

cleaned AS (
    SELECT
        -- 1. Định danh deal & Sản phẩm tham gia
        CAST(deal_id AS BIGINT)                                 AS deal_id,
        CAST(product_id AS BIGINT)                              AS product_id,
        TRIM(product_name)                                      AS product_name,

        -- 2. Giá khuyến mãi & Tỷ lệ giảm giá
        CAST(special_price AS NUMERIC(15, 2))                   AS special_price,
        CAST(original_price AS NUMERIC(15, 2))                  AS original_price,
        CAST(discount_amount AS NUMERIC(15, 2))                 AS discount_amount,
        CAST(discount_percent AS INTEGER)                       AS discount_percent,

        -- 3. Tiến độ mở bán trong khung giờ Flash Sale
        COALESCE(progress_qty, 0)::INTEGER                      AS progress_qty,
        COALESCE(progress_max, 0)::INTEGER                      AS progress_max,

        -- 4. Khung giờ & Trạng thái sự kiện
        TRIM(slot_time)                                         AS slot_time,
        TRIM(status)                                            AS deal_status,

        -- 5. Metadata kiểm toán
        _dlt_load_id                                            AS dlt_load_id,

        -- 6. Khử trùng lặp mã deal
        ROW_NUMBER() OVER (
            PARTITION BY deal_id 
            ORDER BY _dlt_load_id DESC
        ) AS row_num

    FROM source
    WHERE deal_id IS NOT NULL
      AND product_id IS NOT NULL
)

SELECT
    deal_id,
    product_id,
    product_name,
    special_price,
    original_price,
    discount_amount,
    discount_percent,
    progress_qty,
    progress_max,
    slot_time,
    deal_status,
    dlt_load_id
FROM cleaned
WHERE row_num = 1
