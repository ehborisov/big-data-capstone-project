package com.ehborisov.udf
import java.util.Properties

import com.google.common.net.InetAddresses
import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.sql.functions.{col, sum, udf}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._

import scala.io.Source

object CountriesAnalysis {
  def main(args: Array[String]): Unit = {

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

    val networks_csv = Source.fromFile("/data/capstone/geodata/GeoLite2-Country-Blocks-IPv4.csv", "UTF-8")
      .getLines.map(_.split(","))
    val countries_csv = Source.fromFile("/data/capstone/geodata/GeoLite2-Country-Locations.csv", "UTF-8")
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
        (t._1 >= converted) && (converted <= t._2)
      })
      if (country.length > 0) {
        country(0)._3
      }
      else {
        null
      }
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
