package utils

import config.Settings
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Mihai.Petrutiu on 2/6/2017.
  */
object SparkUtils {
  val batchJob = Settings.BatchJob
  val IsDebug = batchJob.isDebug
  var checkpointDirectory = ""

  def getSparkContext(appName: String): SparkContext =  {
    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.casandra.connection.host",batchJob.casandraHost)

    //If we are running from IDE
    if(IsDebug) {
      System.setProperty("hadoop.home.dir", batchJob.hadoopHomeDir)
      conf.setMaster(batchJob.sparkMaster)
      checkpointDirectory = batchJob.checkpointDirectory
    }
    else {}

    //setup spark context
    val context = new SparkContext(conf)
    context
  }

  def getSQLContext(sc: SparkContext): SQLContext = {
    val sqlContext = SQLContext.getOrCreate(sc)
    sc.setCheckpointDir(batchJob.checkpointDirectory)
    sqlContext
  }

  def getStreamingContext(streamingApp: (SparkContext, Duration) => StreamingContext, sc: SparkContext, batchDuration: Duration): StreamingContext = {
    val creatingFunc = () => streamingApp(sc, batchDuration)
    val ssc = sc.getCheckpointDir match {
      case Some(checkpointDir) => StreamingContext.getActiveOrCreate(checkpointDir, creatingFunc, sc.hadoopConfiguration, createOnError = true)
      case None => StreamingContext.getActiveOrCreate(creatingFunc)
    }
    sc.getCheckpointDir.foreach(cp => ssc.checkpoint(cp))
    ssc
  }
}
