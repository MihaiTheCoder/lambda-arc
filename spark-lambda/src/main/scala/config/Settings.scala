package config

/**
  * Created by Mihai.Petrutiu on 1/31/2017.
  */
import com.typesafe.config.ConfigFactory
import org.apache.spark.streaming.Seconds
object Settings {
  private val config = ConfigFactory.load()

  object WebLogGen {
    private val webLogGen = config.getConfig("clickstream")

    lazy val records = webLogGen.getInt("records")
    lazy val timeMultiplier = webLogGen.getInt("time_multiplier")
    lazy val pages = webLogGen.getInt("pages")
    lazy val visitors = webLogGen.getInt("visitors")
    lazy val filePath = webLogGen.getString("file_path")
    lazy val destinationPath = webLogGen.getString("dest_path")
    lazy val numberOfFiles = webLogGen.getInt("number_of_files")
    lazy val topic = webLogGen.getString("topic")
    lazy val bootstrapServersConfig = webLogGen.getString("bootstrap_servers_config") //Seed config
  }

  object BatchJob {
    private val batchJob = config.getConfig("batchJob")

    lazy val isDebug = batchJob.getBoolean("is_debug")

    val sparkAppName = batchJob.getString("spark_app_name")
    val batchDuration = Seconds(batchJob.getInt("batch_duration_in_seconds"))

    val sparkZookeeperConnect = batchJob.getString("spark_client.zookeeper_connect")
    val sparkGroupId = batchJob.getString("spark_client.group_id")

    var checkpointDirectory = ""
    var hadoopHomeDir = ""
    var sparkMaster = ""
    var filePath = ""
    var destinationPath = ""
    if (isDebug) {
      val debugConfig = batchJob.getConfig("debug")
      hadoopHomeDir = debugConfig.getString("hadoop_home_dir")
      checkpointDirectory = debugConfig.getString("checkpoint_directory")
      sparkMaster = debugConfig.getString("spark_master")
      filePath = WebLogGen.filePath
      destinationPath = WebLogGen.destinationPath
    } else {
      val releaseConfig = batchJob.getConfig("release")
      checkpointDirectory = releaseConfig.getString("checkpointDirectory")
      filePath = releaseConfig.getString("file_path")
      destinationPath = releaseConfig.getString("dest_path")
    }
  }

}
