package streaming

import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}
import _root_.kafka.common.TopicAndPartition
import _root_.kafka.message.MessageAndMetadata
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import functions._
import com.twitter.algebird.HyperLogLogMonoid
import config.Settings
import domain._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import utils.SparkUtils


/**
  * Created by Mihai.Petrutiu on 2/6/2017.
  */
object StreamingJob {
  val batchDurationInSeconds = Settings.BatchJob.batchDuration.milliseconds / 1000
  val hdfsPath = Settings.BatchJob.hdfsPath
  val topic = Settings.WebLogGen.topic

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


    val kafkaDirectParams = Map(
      "metadata.broker.list" -> Settings.WebLogGen.bootstrapServersConfig,
      "group.id" -> Settings.BatchJob.sparkGroupId,
      "auto.offset.reset" -> "smallest"
    )

    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    try {
      val hdfsData = sqlContext.read.parquet(hdfsPath)

      fromOffsets = hdfsData
        .groupBy(Activity.topic, Activity.kafkaPartition)
        .agg(max(Activity.untilOffset).as(Activity.untilOffset))
        .collect().map { row =>
        (TopicAndPartition(row.getAs[String](Activity.topic), row.getAs[Int](Activity.kafkaPartition)),
          row.getAs[String](Activity.untilOffset).toLong + 1)
      }.toMap
    } catch {
      case e: Exception => e.printStackTrace()
    }

    val kafkaDirectStream = fromOffsets.isEmpty match {
      case true =>
        println("No Previous result")
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
          ssc, kafkaDirectParams, Set(topic)
        )
      case false =>
        println("With previous results")
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
          ssc, kafkaDirectParams, fromOffsets, { mmd : MessageAndMetadata[String, String] => (mmd.key(), mmd.message()) }
        )
    }

    val activityStream = kafkaDirectStream.transform(input => stringRDDtoActivityRDD(input)).cache()


    activityStream.foreachRDD(rdd => {
      val activityDF = rdd.toDF().selectExpr(ActivityFactory.getActivityWithOffserRangeColumns: _*)

      activityDF
        .write
        .partitionBy(Activity.topic, Activity.kafkaPartition, Activity.timestamp_hour)
        .mode(SaveMode.Append)
        .parquet(hdfsPath)
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
