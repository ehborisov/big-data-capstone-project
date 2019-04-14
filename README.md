# Hadoop developer capstone project:

This project contains data ingestion pipleine configuration and analysis scrips for a small sample project
of data processing using Hadoop, Flume, Hive and Spark in the Cloudera quickstart VM.

## Usage:

1) Start Flume agent with configuration provided in the module `flume-conf`

2) Start events-producing python script in the `py_events_producer`

3) Generate the amount of data you'd like to process. (Flume will output generated events in
 `/user/cloudera/flume/events/%Y/%m/%d` directory in HDFS using regex interceptors for timestamp column)

4) Start hive commandline or Hue and execute SQL scripts from `hive_queries`

5) To execute the last query which requires UDFs, build project with gradle `./gradlew clean build shadowJar`
and place jars into Hive auxiliary jars directory, and then add them and execute `create function` commands.

6) For the spark part use the Jar from the `scala_spark_code` module and use the following spark-submit command.
```bash
spark-submit --class com.ehborisov.udf.PurchasesAnalysisDF --executor-memory 512m --driver-memory 512m  \
--conf spark.purchases.events.dir="hdfs://localhost:8020/user/cloudera/flume/events/year=2019/month=04/day={04,05,06,07,08,09,10}/" \
--conf spark.networks.table="/data/capstone/geodata/GeoLite2-Country-Blocks-IPv4.csv" \
--conf spark.countries.table="/data/capstone/geodata/GeoLite2-Country-Locations.csv" \
--conf spark.jdbc.url="jdbc:postgresql://127.0.0.1:5432/capstone" --conf spark.db.user="admin" \
--conf spark.db.password="12345" \
/data/capstone/scala_spark_code/build/libs/purchases_analysis-1.0-SNAPSHOT-all.jar
```

it expects postgres database named `capstone` to be available at the default port to export the results. 

7) To export the results of hive queries into the same database use SQL scripts and Sqoop commands from the `sqoop`
module.

