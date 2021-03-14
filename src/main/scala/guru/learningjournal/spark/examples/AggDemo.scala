package guru.learningjournal.spark.examples
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.language.implicitConversions


object AggDemo extends App {

  @transient val logger : Logger = Logger.getLogger(getClass)




   val spark = SparkSession.builder().master("local[5]").appName("AggDemo").getOrCreate()

    val invoiceDF = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("data/invoices.csv")

//  invoiceDF.show

  //invoiceDF.select(count("*").as("count"),sum("Quantity").as("TotalQuantity")).show()
   //scala.io.StdIn.readLine()

  // invoiceDF.createOrReplaceTempView("sales")
/*
   val summaryDF = spark.sql(
    """
      |select Country, InvoiceNo , sum(Quantity) as TotalQuantity , round (sum( Quantity * UnitPrice ) , 2) as TotalPrice
      |from sales
      |group by Country, InvoiceNo
      |""".stripMargin)
*/
  // summaryDF.show

   //val  sumDF = invoiceDF.groupBy(col("Country") ,col("InvoiceNo") ).count()

   // sumDF.show
/*

  val  sumDF = invoiceDF.groupBy(col("Country") ,col("InvoiceNo") )
    .agg(count("*").as("count_no"), sum(col("Quantity")).as("TotalQuantity"),
      round(sum( expr("Quantity * UnitPrice")),2).as("TotalPrice")).show()
*/

  //invoiceDF.printSchema()
    val invoiceConvDF = invoiceDF.withColumn("InvoiceDate", to_date(col("InvoiceDate"),"dd-MM-yyyy H.mm"))
  //invoiceConvDF.printSchema()


    // val numInvoices = countDistinct(col("InvoiceNo")).as("numInvoices")
     //val totalQuantity1 = sum(col("Quantity")).as("totalQuantity1")
     //val invoiceValue = expr("round(sum(Quantity * UnitPrice),2) as invoiceValue")

   val SummaryDF = invoiceConvDF.filter(" year(InvoiceDate) == 2010")
      .withColumn("WeekNumber",weekofyear(col("InvoiceDate")))
      .groupBy(col("Country"),col("WeekNumber")).count()
    // .agg(numInvoices)
     //.agg(numInvoices,totalQuantity1,invoiceValue)

  SummaryDF.show()
  /*

   val runningTotalWindow = Window.partitionBy(col("Country"))
     .orderBy(col("WeekNumber").asc_nulls_first)
       .rowsBetween(Window.unboundedPreceding,Window.currentRow)


  SummaryDF.withColumn("TotalRunning Toal",sum(col("invoiceValue")) over (runningTotalWindow)).show

 */
  scala.io.StdIn.readLine()
  spark.stop()

}
