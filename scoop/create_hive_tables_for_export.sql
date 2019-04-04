CREATE TABLE categories_top
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
AS SELECT
    product_category,
    categories_enumerated.cat_count as purchases_by_category
FROM
    (SELECT
        product_category,
        categories.cat_count,
        row_number() over (ORDER BY categories.cat_count DESC) AS row_num
     FROM
        (SELECT
            product_category,
            count(*) AS cat_count
         FROM purchases GROUP BY product_category) as categories
    ) as categories_enumerated
where categories_enumerated.row_num <= 10;


CREATE TABLE top_products_for_each_category
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
AS SELECT
    product_category,
    key as product,
    value as purchases_count,
    rank_num as rank
FROM
    (SELECT
        product_category,
        key,
        value,
        rank() OVER(PARTITION BY product_category ORDER BY value DESC) AS rank_num
    FROM
        (SELECT
             product_category,
             str_to_map(concat_ws(',', collect_list(concat(product_name,':', cast(products_by_cat.purchases_count as string))))) purchases_top
         FROM
             (SELECT product_category,
                 product_name,
                 count(*) AS purchases_count
                 FROM purchases
                 GROUP BY product_name, product_category) products_by_cat
         GROUP BY product_category) t
    LATERAL VIEW explode(t.purchases_top) t AS key,value) t2
WHERE rank_num <= 3