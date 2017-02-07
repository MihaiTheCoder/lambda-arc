package batch

import java.lang.management.ManagementFactory

import config.Settings
import org.apache.spark.{SparkConf, SparkContext}
import domain._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode}
import utils.SparkUtils
/**
  * Created by Mihai.Petrutiu on 1/31/2017.
  */
object BatchJob {
  val tsvFile = Settings.BatchJob.filePath
  def main(args: Array[String]): Unit = {

    val sc = SparkUtils.getSparkContext(Settings.BatchJob.sparkAppName)
    val sqlContext = SparkUtils.getSQLContext(sc)
    //initialize input RDD
    val sourceFile = tsvFile
    val input = sc.textFile(sourceFile)

    createDF(sc, input)
  }

  private def createDF(sc: SparkContext, input: RDD[String]) = {
    implicit val sqlContext = new SQLContext(sc)

    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    val inputDF = input
      .flatMap(line => ActivityFactory.getActivity(line))
      .toDF().cache()

    val df = inputDF.select(
      add_months(from_unixtime(inputDF("timestamp_hour") / 1000), 1).as("timestamp_month"),
      inputDF("referrer"), inputDF("action"), inputDF("prevPage"), inputDF("page"), inputDF("visitor"), inputDF("product")
    )

    df.registerTempTable("activity")

    val visitorByProduct = sqlContext.sql(
      """SELECT product, timestamp_month, COUNT(DISTINCT visitor) as unique_visitors
        |FROM activity GROUP BY product, timestamp_month
      """.stripMargin)

    val activityByProduct = sqlContext.sql(
      """SELECT
                                            product,
                                            timestamp_month,
                                            sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
                                            sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                                            sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
                                            from activity
                                            group by product, timestamp_month """).cache()

    activityByProduct.write.partitionBy("timestamp_month").mode(SaveMode.Append).parquet("hdfs://lambda-pluralsight:9000/lambda/batch1")

    visitorByProduct.foreach(println)
    activityByProduct.foreach(println)
  }
}
