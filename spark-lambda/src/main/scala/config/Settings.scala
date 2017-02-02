package config

/**
  * Created by Mihai.Petrutiu on 1/31/2017.
  */
import com.typesafe.config.ConfigFactory
object Settings {
  private val config = ConfigFactory.load()

  object WebLogGen {
    private val webLogGen = config.getConfig("clickstream")

    lazy val records = webLogGen.getInt("records")
    lazy val timeMultiplier = webLogGen.getInt("time_multiplier")
    lazy val pages = webLogGen.getInt("pages")
    lazy val visitors = webLogGen.getInt("visitors")
    lazy val filePath = webLogGen.getString("file_path")
    lazy val isDebug = webLogGen.getBoolean("is_debug")
  }
}
