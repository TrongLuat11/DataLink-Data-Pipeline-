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
