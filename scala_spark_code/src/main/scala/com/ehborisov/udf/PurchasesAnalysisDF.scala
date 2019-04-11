package com.ehborisov.udf

import com.google.common.net.InetAddresses
import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, rank, sum, udf}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import com.google.common.collect.ImmutableRangeMap
import com.google.common.collect.Range
import org.apache.spark.sql.hive.HiveContext

import scala.io.Source

object PurchasesAnalysisDF {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Purchases analysis")
    val sparkContext = new SparkContext(conf)
    val sql = new HiveContext(sparkContext)
    val properties = System.getProperties

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

    //top 10 most frequently purchased categories

    val top_categories = purchases
      .select("product_name", "product_category")
      .groupBy("product_category")
      .agg(count("product_name").alias("purchases_by_category"))
      .orderBy(col("purchases_by_category").desc)
      .limit(10)

    top_categories.show()

    // Select top 3 most frequently purchased product in each category

    val window = Window.partitionBy(col("product_category")).orderBy(col("purchases_count").desc)

    val top_3_products = purchases
      .select("product_name", "product_category")
      .groupBy("product_category", "product_name")
      .agg(count("product_name").alias("purchases_count"))
      .withColumn("rank", rank().over(window))
      .filter(col("rank") === 3)

    top_3_products.show()

    // Select top 10 countries with the highest money spending using ip addresses table

    val networks_csv = Source.fromFile("/home/eborisov/GeoLite2-Country-Blocks-IPv4.csv", "UTF-8")
      .getLines.map(_.split(","))
    val countries_csv = Source.fromFile("/home/eborisov/GeoLite2-Country-Locations.csv", "UTF-8")
      .getLines.map(_.split(","))
    networks_csv.next // skip header
    countries_csv.next // skip header

    val networks = networks_csv.map(item => (item(1), item(0))).toArray
    val countries_map = countries_csv.map(item => (item(0), item(5))).toMap
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

    //    val data = country_by_network.keySet
    //      .map(k => {
    //      val network_range = extract_network_range(k)
    //      (Range.open(
    //        convert_ip(network_range._1): java.lang.Integer,
    //        convert_ip(network_range._2): java.lang.Integer),
    //        country_by_network(k): java.lang.String
    //      )
    //    }).toArray
    //
    //    val rangeMapBuilder = ImmutableRangeMap.builder[Integer, String]
    //    for (t <- data) {
    //      rangeMapBuilder.put(t._1, t._2)
    //    }
    //
    //    val networks_range_map = spark.sparkContext.broadcast(rangeMapBuilder.build())
    //
    //    val get_ip_country = (s: String) => {
    //      networks_range_map.value.get(convert_ip(s))
    //    }

    val data = network_by_country
      .map(k => {
        val network_range = extract_network_range(k._1)
        (
          convert_ip(network_range._1),
          convert_ip(network_range._2),
          k._2
        )
      }).toArray

    def getFirstOrNone(a: Array[(Int, Int, String)]): String = {
      if (a.isEmpty) {
        null
      }
      else {
        a(0)._3
      }
    }

    val get_ip_country = (s: String) => {
      val converted = convert_ip(s)
      getFirstOrNone(data.filter(t => {
        t._1 >= converted && converted <= t._2
      }))
    }

    val ip_country = udf(get_ip_country)

    val top_countries_by_spending = purchases
      .select("price", "ip_address")
      .withColumn("country", ip_country(col("ip_address")))
      .filter(col("country").isNotNull)
      .groupBy("country")
      .agg(sum("price").alias("total_spending"))
      .orderBy(col("total_spending").desc)
      .limit(10)

    top_countries_by_spending.show()
  }
}
