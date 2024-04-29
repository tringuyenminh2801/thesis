select
    ROW_NUMBER() OVER (
        ORDER BY
            product_name
    ) + (
        SELECT
            ifnull (max(id), 0)
        FROM
            mid
    ) id,
    product_name,
    price_class,
    brand_lv1,
    brand_lv2
from
    src