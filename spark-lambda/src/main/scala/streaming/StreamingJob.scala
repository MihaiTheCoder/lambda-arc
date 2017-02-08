package streaming

import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import functions._
import com.twitter.algebird.HyperLogLogMonoid
import config.Settings
import domain._
import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel
import utils.SparkUtils


/**
  * Created by Mihai.Petrutiu on 2/6/2017.
  */
object StreamingJob {
  val batchDurationInSeconds = Settings.BatchJob.batchDuration.milliseconds / 1000

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

    val kafkaParams = Map(
      "zookeeper.connect" -> Settings.BatchJob.sparkZookeeperConnect,
      "group.id" -> Settings.BatchJob.sparkGroupId,
      "auto.offset.reset" -> "largest"
    )

    val kafkaDirectPrams = Map(
      "metadata.broker.list" -> Settings.WebLogGen.bootstrapServersConfig,
      "group.id" -> Settings.BatchJob.sparkGroupId,
      "auto.offset.reset" -> "smallest"
    )

    val kafkaDirectStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaDirectPrams, Set(Settings.WebLogGen.topic)
    )

    val activityStream = kafkaDirectStream.transform(input => stringRDDtoActivityRDD(input)).cache()

    activityStream.foreachRDD(rdd => {
      val activityDF = rdd.toDF().selectExpr(ActivityFactory.getActivityWithOffserRangeColumns: _*)
      activityDF
        .write
        .partitionBy(Activity.topic, Activity.kafkaPartition, Activity.timestamp_hour)
        .mode(SaveMode.Append)
        .parquet(s"${Settings.BatchJob.hadoop}/webblogs-app1/")
    })

    val activityStateSpec = StateSpec
      .function(mapActivityStateFunc)
      .timeout(Minutes(120))

    val statefulActivityByProduct = activityStream.transform(rdd => {
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
    }).mapWithState(activityStateSpec)

    val activityStateSnapshot = statefulActivityByProduct.stateSnapshots()
    activityStateSnapshot.reduceByKeyAndWindow(
      (a, b) => b,
      (x, y) => x,
      Seconds(30 / batchDurationInSeconds * batchDurationInSeconds)
    )
      .foreachRDD(rdd => rdd.map(sr => ActivityByProduct(sr._1._1, sr._1._2, sr._2._1, sr._2._2, sr._2._3))
        .toDF().registerTempTable("ActivityByProduct"))


    //unique visitors by product
    val visitorStateSpec = StateSpec
      .function(mapVisitorsStateFunc)
      .timeout(Minutes(120))

    val hll = new HyperLogLogMonoid(12)
    val statefulVisitorsByProduct = activityStream.map(a => {
      ((a.product, a.timestamp_hour), hll(a.visitor.getBytes))
    }).mapWithState(visitorStateSpec)

    val visitorStateSnapshot = statefulVisitorsByProduct.stateSnapshots()


    visitorStateSnapshot
      .reduceByKeyAndWindow(
        (a, b) => b,
        (x, y) => x,
        Seconds(30 / batchDurationInSeconds * batchDurationInSeconds)
      ) // only save or expose the snapshot every x seconds - 28
      .foreachRDD(rdd => rdd
      .map(sr => VisitorByProduct(sr._1._1, sr._1._2, sr._2.approximateSize.estimate)).toDF
      .registerTempTable("VisitorsByProduct"))


    //statefulVisitorsByProduct.print(10)
    ssc
  }
}
