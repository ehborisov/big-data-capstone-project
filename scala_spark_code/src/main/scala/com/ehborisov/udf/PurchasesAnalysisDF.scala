package com.ehborisov.udf

import com.google.common.net.InetAddresses
import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, rank, sum, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import com.google.common.collect.ImmutableRangeMap
import com.google.common.collect.Range;

import scala.io.Source

object PurchasesAnalysisDF {
  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("Purchases analysis").getOrCreate()

    val PURCHASES_SCHEMA = StructType(Seq(
      StructField("product_name", StringType),
      StructField("price", DoubleType),
      StructField("purchase_date", TimestampType),
      StructField("product_category", StringType),
      StructField("ip_address", StringType),
      StructField("year", IntegerType),
      StructField("month", IntegerType),
      StructField("day", IntegerType)))


    val purchases = spark.read.format("csv")
      .option("delimiter", ",")
      .option("header", "false")
      .schema(PURCHASES_SCHEMA)
      .load("hdfs://localhost:8020/user/cloudera/flume/events")

    //top 10 most frequently purchased categories

    val top_categories = purchases
      .select("price", "product_category")
      .groupBy("product_category")
      .agg(sum("price").alias("total_spending"))
      .orderBy(col("total_spending").desc)
      .limit(10)

    top_categories.show()

    // Select top 3 most frequently purchased product in each category

    val window = Window.partitionBy(col("product_category")).orderBy(col("total_spending").desc)

    val top_3_products = purchases
      .select("product_name", "price", "product_category")
      .groupBy("product_category", "product_name")
      .agg(sum("price").alias("total_spending"))
      .withColumn("rank", rank().over(window))
      .filter(col("rank") === 3)
      .drop("rank")

    top_3_products.show()

    // Select top 10 countries with the highest money spending using ip addresses table

    val networks_csv = Source.fromFile("/data/capstone/geodata/GeoLite2-Country-Blocks-IPv4.csv")
      .getLines.map(_.split(","))
    val countries_csv = Source.fromFile("/data/capstone/geodata/GeoLite2-Country-Locations.csv")
      .getLines.map(_.split(","))
    networks_csv.next // skip header
    countries_csv.next // skip header

    val networks_map = networks_csv.map(item => (item(1), item(0))).toMap
    val countries_map = countries_csv.map(item => (item(0), item(5))).toMap
    val country_by_network = networks_map.keySet.intersect(countries_map.keySet)
      .map(k => networks_map(k) -> countries_map(k))
      .toMap

    val convert_ip = (s: String) => {
      InetAddresses.coerceToInteger(InetAddresses.forString(s))
    }

    val extract_network_range = (s: String) => {
      val utils = new SubnetUtils(s)
      (utils.getInfo.getLowAddress, utils.getInfo.getHighAddress)
    }

    val data = country_by_network.keySet
      .filter(k => {
        val network_range = extract_network_range(k)
        convert_ip(network_range._1) != convert_ip(network_range._2)
      })
      .map(k => {
      val network_range = extract_network_range(k)
      (Range.open(
        convert_ip(network_range._1): java.lang.Integer,
        convert_ip(network_range._2): java.lang.Integer),
        country_by_network(k): java.lang.String
      )
    }).toArray

    val rangeMapBuilder = ImmutableRangeMap.builder[Integer, String]
    for (t <- data) {
      rangeMapBuilder.put(t._1, t._2)
    }

    val networks_range_map = spark.sparkContext.broadcast(rangeMapBuilder.build())

    val get_ip_country = (s: String) => {
      networks_range_map.value.get(convert_ip(s))
    }


    val ip_country = udf(get_ip_country)

    val top_countries_by_spending = purchases
      .select("price", "ip_address")
      .withColumn("country", ip_country(col("ip_address")))
      .filter(col("country").notEqual(""))
      .groupBy("country")
      .agg(sum("price").alias("total_spending"))
      .orderBy(col("total_spending").desc)
      .limit(10)

    top_countries_by_spending.show()
  }
}
