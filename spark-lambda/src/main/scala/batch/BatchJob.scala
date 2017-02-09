package batch

import java.lang.management.ManagementFactory

import config.Settings
import org.apache.spark.{SparkConf, SparkContext}
import domain._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import utils.SparkUtils
/**
  * Created by Mihai.Petrutiu on 1/31/2017.
  */
object BatchJob {
  val tsvFile = Settings.BatchJob.filePath
  val hdfsPath = Settings.BatchJob.hdfsPath
  def main(args: Array[String]): Unit = {

    val sc = SparkUtils.getSparkContext(Settings.BatchJob.sparkAppName)
    val sqlContext = SparkUtils.getSQLContext(sc)
    //initialize input RDD
    val inputDF = sqlContext.read.parquet(hdfsPath)
      .where("unix_timestamp() - timestamp_hour/1000 <= 60*60*6")

    createDF(sc, inputDF)
  }

  private def createDF(sc: SparkContext, input: DataFrame) = {
    implicit val sqlContext = new SQLContext(sc)

    import org.apache.spark.sql.functions._
    import sqlContext.implicits._


    input.registerTempTable("activity")

    val visitorByProduct = sqlContext.sql(
      """SELECT product, timestamp_hour, COUNT(DISTINCT visitor) as unique_visitors
        |FROM activity GROUP BY product, timestamp_hour
      """.stripMargin)

    val activityByProduct = sqlContext.sql(
      """SELECT
                                            product,
                                            timestamp_hour,
                                            sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
                                            sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                                            sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
                                            from activity
                                            group by product, timestamp_hour """).cache()

    activityByProduct.write.partitionBy("timestamp_hour").mode(SaveMode.Append).parquet("hdfs://lambda-pluralsight:9000/lambda/batch1")

    visitorByProduct.foreach(println)
    activityByProduct.foreach(println)
  }
}
