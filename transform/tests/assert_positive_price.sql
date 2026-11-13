-- tests/assert_positive_price.sql
-- Kiểm tra không có sản phẩm nào có giá bán âm hoặc bằng 0
-- Trả về các dòng vi phạm (nếu kết quả có > 0 dòng thì test FAIL)
SELECT
    product_id,
    product_name,
    price
FROM {{ ref('stg_products') }}
WHERE price <= 0
