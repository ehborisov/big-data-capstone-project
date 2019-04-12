package com.ehborisov.udf

import java.util.Properties

import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, rank, sum, udf}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object PurchasesAnalysisDF {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Purchases analysis")
    val sparkContext = new SparkContext(conf)
    val sql = new HiveContext(sparkContext)
    val properties = System.getProperties
    val jdbc_properties = new Properties()
    jdbc_properties.setProperty("driver", "org.postgresql.Driver")

    val PURCHASES_SCHEMA = StructType(Seq(
      StructField("product_name", StringType),
      StructField("price", DoubleType),
      StructField("purchase_date", TimestampType),
      StructField("product_category", StringType),
      StructField("ip_address", StringType),
      StructField("year", IntegerType),
      StructField("month", IntegerType),
      StructField("day", IntegerType)))

    val purchases = sql.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "false")
      .schema(PURCHASES_SCHEMA)
      .load(properties.getProperty("spark.purchases.events.dir"))

    val networks = sql.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(properties.getProperty("spark.networks.table"))

    val countries = sql.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(properties.getProperty("spark.countries.table"))

    //top 10 most frequently purchased categories

    purchases
      .select("product_name", "product_category")
      .groupBy("product_category")
      .agg(count("product_name").alias("purchases_by_category"))
      .orderBy(col("purchases_by_category").desc)
      .limit(10)
      .write
      .mode("overwrite")
      .jdbc(properties.getProperty("spark.jdbc.url"),
        "categories_top_from_spark", jdbc_properties)

    // Select top 3 most frequently purchased product in each category

    val window = Window.partitionBy(col("product_category"))
      .orderBy(col("purchases_count").desc)

    purchases
      .select("product_name", "product_category")
      .groupBy("product_category", "product_name")
      .agg(count("product_name").alias("purchases_count"))
      .withColumn("rank", rank().over(window))
      .filter(col("rank") === 3)
      .write
      .mode("overwrite")
      .jdbc(properties.getProperty("spark.jdbc.url"),
        "top_products_for_each_category_from_spark", jdbc_properties)

    // Select top 10 countries with the highest money spending using ip addresses table


    val is_in_range = (network: String, ip: String) => {
      try {
        new SubnetUtils(network).getInfo.isInRange(ip)
      } catch {
        case e: Exception => false
      }
    }

    val ip_in_range_udf = udf(is_in_range)

    purchases
      .join(networks.alias("networks"),
        ip_in_range_udf(col("networks.network"), col("ip_address")))
      .join(countries.alias("countries"),
        col("networks.geoname_id") === col("countries.geoname_id"))
      .groupBy("country_name")
      .agg(sum("price").alias("total_spending"))
      .orderBy(col("total_spending").desc)
      .limit(10)
      .write
      .mode("overwrite")
      .jdbc(properties.getProperty("spark.jdbc.url"), "top_countries_from_spark", jdbc_properties)
  }
}
