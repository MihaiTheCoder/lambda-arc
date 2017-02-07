package streaming

import config.Settings
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Duration, StreamingContext}
import utils.SparkUtils

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

    val textDStream = ssc.textFileStream(Settings.BatchJob.destinationPath)

    textDStream.print()

    ssc
  }
}
