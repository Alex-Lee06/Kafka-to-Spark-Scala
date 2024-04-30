package org.microsoft.kafkaSpark.lab

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, from_json}
import org.slf4j.{Logger, LoggerFactory}

import java.io.FileNotFoundException
import java.util.Properties
import scala.io.Source

object main {
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


  }
}
