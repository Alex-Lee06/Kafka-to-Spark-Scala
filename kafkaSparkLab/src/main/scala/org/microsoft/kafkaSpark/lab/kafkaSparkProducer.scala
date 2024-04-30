package org.microsoft.kafkaSpark.lab

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, from_json}
import org.slf4j.{Logger, LoggerFactory}


import java.io.FileNotFoundException
import java.util.Properties
import scala.io.Source

object kafkaSparkProducer {
  def main(args: Array[String]): Unit = {

    val url = getClass.getResource("application.properties")
    val properties: Properties = new Properties()
    val logger: Logger = LoggerFactory.getLogger(getClass)

    if (url != null) {
      val source = Source.fromURL(url)
      properties.load(source.bufferedReader())

    }
    else {
      logger.error("properties file cannot be found")
      throw new FileNotFoundException("Properties file cannot be loaded")
    }

    val brokerServers = properties.getProperty("broker_servers")
    val topics = properties.getProperty("topic")
    val startingOffset = properties.getProperty("starting_offset")

    val spark = SparkSession.builder()
      .master("yarn")
      .appName("HDInsight Kafka AKS").getOrCreate()

/*
    val host = "127.0.0.1"
    val port = "9999"

    val df = spark.readStream
      .format("socket")
      .format("host", host)
      .format(("port", port)
      .load()


 */

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerServers)
      .option("subscribe", topics)
      .option("startingOffsets", startingOffset) // From starting
      .load()

    val personStringDf = df.selectExpr("CAST(value As STRING)")

    val schema = new StructType()
      .add("id",IntegerType)
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType)
      .add("dob_year",IntegerType)
      .add("dob_month",IntegerType)
      .add("gender",StringType)
      .add("salary",IntegerType)

    val personDF = personStringDf.select(from_json(col("value"), schema).as("data"))
      .select("data.*")

    personDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()


  }
}