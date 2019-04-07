from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import StructType, StringType, StructField, DoubleType, TimestampType, IntegerType
from pyspark.sql import functions as f
from pyspark.sql.window import Window

sc = SparkContext()
spark = SparkSession.builder \
    .master("local") \
    .appName("Purchases analysis") \
    .getOrCreate()
sql = SQLContext(sc, sparkSession=spark)

PURCHASES_SCHEMA = StructType([
    StructField("product_name", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("purchase_date", TimestampType(), True),
    StructField("product_category", StringType(), True),
    StructField('ip_address', StringType(), True),
    StructField('year', IntegerType(), True),
    StructField('month', IntegerType(), True),
    StructField('day', IntegerType(), True),
])


purchases = sql.read.csv('hdfs://localhost:8020/user/cloudera/flume/events',
                         sep=',', header=False, schema=PURCHASES_SCHEMA)
networks = sql.read.csv('hdfs://localhost:8020/user/cloudera/GeoLite2-Country-Blocks-IPv4.csv',
                        sep=',', header=True, inferSchema=True)
countries = sql.read.csv('hdfs://localhost:8020/user/cloudera/GeoLite2-Country-Locations.csv',
                         sep=',', header=True, inferSchema=True)

# top 10 most frequently purchased categories

top_categories = purchases\
    .select(purchases.price, purchases.product_category)\
    .groupBy('product_category').agg(f.sum('price').alias('total_spending'))\
    .orderBy(f.col('total_spending').desc())

top_10_categories = top_categories.take(10)

# Select top 3 most frequently purchased product in each category

window = Window.partitionBy(purchases.product_category).orderBy(f.col('total_spending').desc())

top_3_products = purchases\
    .select(purchases.product_name, purchases.price, purchases.product_category)\
    .groupBy([purchases.product_category, purchases.product_name])\
    .agg(f.sum('price').alias('total_spending'))\
    .withColumn('rank', f.rank().over(window))\
    .filter(f.col('rank') == 3)\
    .drop('rank')

for row in top_3_products.collect():
    print(row)




