package streaming

import config.Settings
import domain.{ActivityByProduct, ActivityByProductFactory, ActivityFactory}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Duration, StreamingContext}
import utils.SparkUtils
import org.apache.spark.sql.functions._
/**
  * Created by Mihai.Petrutiu on 2/6/2017.
  */
object StreamingJob {
  def main(args: Array[String]): Unit = {
    println(Settings.BatchJob.isDebug)
    val sc = SparkUtils.getSparkContext(Settings.BatchJob.sparkAppName)
    val ssc = SparkUtils.getStreamingContext(streamingApp, sc, Settings.BatchJob.batchDuration)

    ssc.start()
    ssc.awaitTermination()
  }

  def streamingApp(sc: SparkContext, batchDuration: Duration): StreamingContext = {
    val ssc = new StreamingContext(sc, batchDuration)
    val sqlContext = SparkUtils.getSQLContext(sc)
    import sqlContext.implicits._
    val textDStream = ssc.textFileStream(Settings.BatchJob.destinationPath)

    val activityStream = textDStream.transform(input => input.flatMap(line => ActivityFactory.getActivity(line)))

    activityStream.transform(rdd => {
      val df = rdd.toDF()

      df.registerTempTable("activity")

      val activityByProduct = sqlContext.sql(
        """SELECT
                                            product,
                                            timestamp_hour,
                                            sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
                                            sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                                            sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
                                            from activity
                                            group by product, timestamp_hour """)

      activityByProduct.map(r => ((r.getString(0), r.getLong(1)), ActivityByProductFactory.getActivityByProduct(r)))
    }).print()

    //textDStream.print()

    ssc
  }
}
