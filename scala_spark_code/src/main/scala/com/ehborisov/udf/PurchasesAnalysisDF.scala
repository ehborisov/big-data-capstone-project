package com.ehborisov.udf

import java.util.Properties

import com.google.common.net.InetAddresses
import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, rank, sum, udf}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import scala.io.Source

object PurchasesAnalysisDF {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Purchases analysis")
    val sparkContext = new SparkContext(conf)
    val sql = new HiveContext(sparkContext)
    val properties = System.getProperties
    val jdbc_properties = new Properties()
    jdbc_properties.setProperty("driver", "org.postgresql.Driver")
    jdbc_properties.setProperty("user", properties.getProperty("spark.db.user"))
    jdbc_properties.setProperty("password", properties.getProperty("spark.db.password"))


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

//    val networks = sql.read.format("com.databricks.spark.csv")
//      .option("delimiter", ",")
//      .option("header", "true")
//      .option("inferSchema", "true")
//      .load(properties.getProperty("spark.networks.table"))
//
//    val countries = sql.read.format("com.databricks.spark.csv")
//      .option("delimiter", ",")
//      .option("header", "true")
//      .option("inferSchema", "true")
//      .load(properties.getProperty("spark.countries.table"))

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


//    val is_in_range = (network: String, ip: String) => {
//      try {
//        new SubnetUtils(network).getInfo.isInRange(ip)
//      } catch {
//        case e: Exception => false
//      }
//    }
//
//    val ip_in_range_udf = udf(is_in_range)
//
//    purchases
//      .join(networks.alias("networks"),
//        ip_in_range_udf(col("networks.network"), col("ip_address")))
//      .join(countries.alias("countries"),
//        col("networks.geoname_id") === col("countries.geoname_id"))
//      .groupBy("country_name")
//      .agg(sum("price").alias("total_spending"))
//      .orderBy(col("total_spending").desc)
//      .limit(10)
//      .write
//      .mode("overwrite")
//      .jdbc(properties.getProperty("spark.jdbc.url"), "top_countries_from_spark", jdbc_properties)


    val networks_csv = Source.fromFile(properties.getProperty("spark.networks.table"), "UTF-8")
      .getLines.map(_.split(","))
    val countries_csv = Source.fromFile(properties.getProperty("spark.countries.table"), "UTF-8")
      .getLines.map(_.split(","))
    networks_csv.next // skip header
    countries_csv.next // skip header

    val networks = networks_csv.map(item => (item(1), item(0))).toArray
    val countries_map = countries_csv
      .map(item => (item(0), item(5).replaceAll("^\"|\"$", "")))
      .filter(item => item._2.length > 0).toMap
    val network_by_country = countries_map.keySet.flatMap(
      k => {
        val per_country = networks.filter(n => n._1 == k)
        per_country.map(n => (n._2, countries_map(k)))
      }
    )

    val convert_ip = (s: String) => {
      InetAddresses.coerceToInteger(InetAddresses.forString(s))
    }

    val extract_network_range = (s: String) => {
      val utils = new SubnetUtils(s)
      (utils.getInfo.getLowAddress, utils.getInfo.getHighAddress)
    }

    val data = network_by_country
      .map(k => {
        val network_range = extract_network_range(k._1)
        (
          convert_ip(network_range._1),
          convert_ip(network_range._2),
          k._2
        )
      }).toArray

    val get_ip_country = (s: String) => {
      val converted = convert_ip(s)
      val country = data.filter(t => {
        (converted >= t._1) && (converted <= t._2)
      })
      if (country.length > 0) {
        country(0)._3
      }
      else {
        null
      }
    }

    val ip_country = udf(get_ip_country)

    purchases
      .select("price", "ip_address")
      .withColumn("country", ip_country(col("ip_address")))
      .filter(col("country").isNotNull)
      .groupBy("country")
      .agg(sum("price").alias("total_spending"))
      .orderBy(col("total_spending").desc)
      .limit(10)
      .write
      .mode("overwrite")
      .jdbc(properties.getProperty("spark.jdbc.url"), "top_countries_from_spark", jdbc_properties)
  }
}
