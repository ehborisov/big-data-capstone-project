-- 1. Create table for purchases data
CREATE EXTERNAL TABLE purchases (
  product_name STRING,
  price DOUBLE,
  purchase_date TIMESTAMP,
  product_category STRING,
  ip_address STRING
)
PARTITIONED BY (year int, month int, day int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/cloudera/flume/events/';

-- 2. Fill the table with Flume events stored in hdfs, creating partitions manually
LOAD DATA INPATH '/user/cloudera/flume/events/2019/03/21/FlumeData.1553797510198' INTO TABLE purchases PARTITION (year= 2019, month = 03, day = 21);
LOAD DATA INPATH '/user/cloudera/flume/events/2019/03/22/FlumeData.1553797510552' INTO TABLE purchases PARTITION (year= 2019, month = 03, day = 22);
LOAD DATA INPATH '/user/cloudera/flume/events/2019/03/23/FlumeData.1553797510638' INTO TABLE purchases PARTITION (year= 2019, month = 03, day = 23);
LOAD DATA INPATH '/user/cloudera/flume/events/2019/03/24/FlumeData.1553797510783' INTO TABLE purchases PARTITION (year= 2019, month = 03, day = 24);
LOAD DATA INPATH '/user/cloudera/flume/events/2019/03/25/FlumeData.1553797506776' INTO TABLE purchases PARTITION (year= 2019, month = 03, day = 25);
LOAD DATA INPATH '/user/cloudera/flume/events/2019/03/26/FlumeData.1553797510384' INTO TABLE purchases PARTITION (year= 2019, month = 03, day = 26);
LOAD DATA INPATH '/user/cloudera/flume/events/2019/03/27/FlumeData.1553797510074' INTO TABLE purchases PARTITION (year= 2019, month = 03, day = 27);

-- 3. Create ip addresses table and load the data from csv
CREATE EXTERNAL TABLE networks (
    network STRING,
    geoname_id INT,
    registered_country_geoname_id INT,
    represented_country_geoname_id INT,
    is_anonymous_proxy INT,
    is_satellite_provider INT
) row format delimited fields terminated by ','
tblproperties ("skip.header.line.count"="1");


LOAD DATA LOCAL INPATH '/data/capstone/events-producer/src/main/resources/GeoLite2-Country-Blocks-IPv4.csv' OVERWRITE INTO TABLE networks;

CREATE EXTERNAL TABLE countries (
    geoname_id INT,
    locale_code STRING,
    continent_code STRING,
    continent_name STRING,
    country_iso_code STRING,
    country_name STRING,
    is_in_european_union INT
) row format delimited fields terminated by ','
tblproperties ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/data/capstone/events-producer/src/main/resources/GeoLite2-Country-Locations-en.csv' OVERWRITE INTO TABLE countries;

-- 4. Select top 10  most frequently purchased categories
SELECT
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

-- 5. Select top 10 most frequently purchased product in each category
SELECT
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
             str_to_map(concat_ws(',', collect_list(concat(product_name,':', cast(products_by_cat.purchases_count AS string))))) purchases_top
         FROM
             (SELECT product_category,
                 product_name,
                 count(*) AS purchases_count
                 FROM purchases
                 GROUP BY product_name, product_category) products_by_cat
         GROUP BY product_category) t
    LATERAL VIEW explode(t.purchases_top) t AS key,value) t2
WHERE rank_num <= 3

-- 6. Select top 10 countries with the highest money spending using ip addresses table


CREATE FUNCTION subnet_range_start AS 'com.ehborisov.udf.SubnetRangeStart';
CREATE FUNCTION subnet_range_end AS 'com.ehborisov.udf.SubnetRangeEnd';
CREATE FUNCTION convert_ip AS 'com.ehborisov.udf.ConvertIp';

-- with UDFs defined above:
-- assuming we would use set mapreduce.input.fileinputformat.split.maxsize=5000000; (depending on the number of workers)
-- to split presumably large purchases table and do a cross join as map join.


WITH p AS
(SELECT
    price,
    convert_ip(ip_address) as ip_address,
    1 as key
 FROM purchases),
n_ranges AS
(SELECT
    subnet_range_start(n.network) as subnet_start,
    subnet_range_end(n.network) as subnet_end,
    country_name,
    1 as key
 FROM networks n JOIN countries c ON n.geoname_id = c.geoname_id)
SELECT
    t.country,
    t.spending
FROM
    (SELECT
        n_ranges.country_name as country,
        sum(p.price) AS spending
    FROM p INNER JOIN n_ranges ON (p.key=n_ranges.key)
    WHERE p.ip_address BETWEEN n_ranges.subnet_start AND n_ranges.subnet_end
    GROUP BY n_ranges.country_name
    ORDER BY spending) as t
LIMIT 10