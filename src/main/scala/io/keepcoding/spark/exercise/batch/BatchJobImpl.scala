package io.keepcoding.spark.exercise.batch

import org.apache.spark.sql.functions.{avg, lit, max, min, window}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.OffsetDateTime

object BatchJobImpl extends BatchJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark Structured Streaming KeepCoding Base")
    .getOrCreate()
  import spark.implicits._
  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    //hacemos un job
    spark
      .read
      .format("parquet")
      .load("/tmp/spark-data")
      .where($"year"===lit(filterDate.getYear)&&
        $"month"=== lit(filterDate.getMonthValue) &&
        $"day" === lit(filterDate.getDayOfMonth)&&
        $"hour" === lit(filterDate.getHour))


  }



  override def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }
  override def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    //join con elemento común que sería el id
    antennaDF.as("a")
      .join(metadataDF.as("b"), $"a.id"=== $"b.id")
      .drop($"b.id")
  }

  override def computeDevicesCountByCoordinates(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"metric",$"value", $"location")
      .where($"metric" === lit("devices_count"))
      .groupBy($"location", window($"timestamp", "1 hour"))
      .agg(
        max("value").as("max_devices_count"),
        min ("value").as ("min_devices_count"),
        avg ("value").as("avg_devices_count")
      )
      .select($"location", $"window.start".as("date"), $"max_devices_count",
        $"min_devices_count",$"avg_devices_count")
  }

  override def computeErrorAntennaByModelAndVersion(dataFrame: DataFrame): DataFrame = ???

  override def computePercentStatusByID(dataFrame: DataFrame): DataFrame = ???

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .write
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .mode(SaveMode.Append)
      .save()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit = ???




  def main (args: Array[String]): Unit ={
    var rawDF= readFromStorage("/tmp/spark-data", OffsetDateTime.parse("2022-02-17T22:00:00Z"))
    val metadataDF = readAntennaMetadata( s"jdbc:postgresql://localhost:5432/postgres",
      "metadata",
      "postgres",
      "mysecretpassword" )
    val enrichDF = enrichAntennaWithMetadata(rawDF, metadataDF)

     writeToJdbc(computeDevicesCountByCoordinates(enrichDF),
       s"jdbc:postgresql://localhost:5432/postgres",
       "antena_1h_agg",
       "postgres",
       "mysecretpassword"

     )


  }
}

