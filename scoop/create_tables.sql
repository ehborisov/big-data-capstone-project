use capstonedb;

CREATE TABLE top_10_categories_by_purchases (
    product_category text,
    purchases_by_category integer
);

CREATE TABLE top_products_for_each_category (
    product_category text,
    product text,
    purchases_count integer,
    product_rank integer
);