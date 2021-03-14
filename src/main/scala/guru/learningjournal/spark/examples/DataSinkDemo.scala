package guru.learningjournal.spark.examples
import org.apache.log4j._
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataSinkDemo  extends Serializable {

  @transient val logger : Logger = Logger.getLogger(getClass)
  def main(args: Array[String]): Unit = {

    if (args.length==0){
      logger.error("Source file is missing")
      System.exit(1)

    }

    val spark = SparkSession.builder().master("local[3]").appName("DataSinktarget").getOrCreate()

    val flightParquetDF = spark.read.format("parquet")
      .load("data/flight-time.parquet")

    import org.apache.spark.sql.functions._

     flightParquetDF.show
     println("number of partitions" + flightParquetDF.rdd.getNumPartitions)
     flightParquetDF.groupBy(spark_partition_id()).count().show()

     val partitionDF = flightParquetDF.repartition(5)
    partitionDF.groupBy(spark_partition_id()).count().show()

     flightParquetDF.write.format("avro").mode(SaveMode.Overwrite).save("dataSink/avro/")

     flightParquetDF.write.format("json").partitionBy("OP_CARRIER", "ORIGIN").mode(SaveMode.Overwrite).option("maxRecordsPerFile",10000).save("dataSink/json/")

     spark.catalog.listTables().show()
     spark.sql("create database if not exists AirlineDB")
     spark.catalog.setCurrentDatabase("AirlineDB")

    flightParquetDF.write.mode(SaveMode.Overwrite).saveAsTable("flight_data_tbl")

    spark.stop()
  }

}
