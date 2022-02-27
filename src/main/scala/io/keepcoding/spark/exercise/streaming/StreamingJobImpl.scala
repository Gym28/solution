package io.keepcoding.spark.exercise.streaming


import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}


object StreamingJobImpl extends StreamingJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark Structured Streaming KeepCoding Base")
    .getOrCreate()
  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
       spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", " localhost:9092")
      .option("subscribe", topic)
      .load()

  }

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
  val struct = StructType(Seq(
    StructField ("timestamp", TimestampType,   nullable = false),
    StructField ("id",StringType, nullable=false),
    StructField ("metric", StringType, nullable = false),
    StructField("value", IntegerType, nullable = false)
  ))

    dataFrame
      .select(from_json($"value".cast(StringType), struct).as("value"))
      .select($"value.*")
  }
  override def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame ={
      spark
        .read
        .format("jdbc")
        .option ("url",jdbcURI)
        .option("dbtable",jdbcTable)
        .option("user", user)
        .option("password",password)
        .load()

  }
  override def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame = {
   //join con elemento común que sería el id
    antennaDF.as("a")
      .join(metadataDF.as("b"), $"a.id"===$"b.id")
      .drop($"b.id")
  }
  override def computeDevicesCountByCoordinates(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"metric",$"value", $"location")
      .where($"metric" === lit("devices_count"))
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"location", window($"timestamp", "90 seconds"))
      .agg(
        max("value").as("max_devices_count"),
        min ("value").as ("min_devices_count"),
        avg ("value").as("avg_devices_count")
    )
      .select($"location", $"window.start".as("date"), $"max_devices_count",
      $"min_devices_count",$"avg_devices_count")
  }



  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]= Future{
    dataFrame
      .writeStream
      .foreachBatch{(data:DataFrame, batchId:Long )=>
        data
          .write
          .format("jdbc")
          .option("url", jdbcURI)
          .option("dbtable", jdbcTable)
          .option("user", user)
          .option("password", password)
          .mode(SaveMode.Append)
          .save()
      } .start()
        .awaitTermination()
  }


  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future{
    //particionamos por año mes día y hora
    dataFrame
      //añadimos las columnas para el particionado
        //timestamp es para obtener el año, mes y dia
      .withColumn("year", year($"timestamp"))
      .withColumn("month", month($"timestamp"))
      .withColumn("day", dayofmonth($"timestamp"))
      .withColumn("hour", hour($"timestamp"))
      .writeStream
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
    //donde voy a escribir
      .option("path", storageRootPath)
      .option("checkpointLocation", "/tmp/spark-checkPoint")
    //hacemos un start
      .start()
      .awaitTermination()

  }



  def main (args: Array[String]): Unit ={
    val metadataDF = readAntennaMetadata(
      s"jdbc:postgresql://localhost:5432/postgres",
      "metadata",
      "postgres",
      "mysecretpassword" )



   val future1=  writeToJdbc(computeDevicesCountByCoordinates(
      enrichAntennaWithMetadata(
        parserJsonData(readFromKafka("192.168.18.123:9092", "antenna_telemetry")),

        metadataDF
      )

    ),s"jdbc:postgresql://localhost:5432/postgres","antenna_agg", "postgres","mysecretpassword")


  val future2 =writeToStorage(parserJsonData(readFromKafka("192.168.18.123:9092", "antenna_telemetry")), "/tmp/spark-data")
  //debemos hacer el Await para lanzar los futuros,
    // hacemos una secuencia de futuros

    Await.result(Future.sequence(Seq(future1, future2)), Duration.Inf)


  }




}
