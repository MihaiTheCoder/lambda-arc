package clickstream

import java.io.FileWriter

import config.Settings

import scala.io.Source
import scala.util.Random

/**
  * Created by Mihai.Petrutiu on 1/31/2017.
  */
object LogProducer extends App {
  val webLog = Settings.WebLogGen

  val products = readResourceFileLineByLine("/products.csv")
  val referrers = readResourceFileLineByLine("/referrers.csv")
  val visitors = (0 to webLog.visitors).map("Visitor-" + _).toArray
  val pages = (0 to webLog.pages).map("Page-" + _).toArray

  val rand = new Random()
  val filePath = webLog.filePath

  val fileWriter = new FileWriter(filePath, true)

  val incrementTimeEvery = rand.nextInt(webLog.records - 1) + 1

  var timestamp = System.currentTimeMillis()
  var adjustedTimestamp = timestamp

  for (iteration <- 1 to webLog.records) {
    adjustedTimestamp = adjustedTimestamp + ((System.currentTimeMillis() - timestamp) * webLog.timeMultiplier)
    timestamp = System.currentTimeMillis()
    val action = iteration % (rand.nextInt(200) + 1) match {
      case 0 => "purchase"
      case 1 => "add_to_cart"
      case _ => "page_view"
    }
    val referrer = getRandomItem(referrers)

    val prevPage = referrer match {
      case "Internal" => getRandomItem(pages)
      case _ => ""
    }

    val visitor = getRandomItem(visitors)
    val page = getRandomItem(pages)
    val product = getRandomItem(products)

    val line = s"$adjustedTimestamp\t$referrer\t$action\t$prevPage\t$visitor\t$page\t$product\n"
    fileWriter.write(line)

    if (iteration % incrementTimeEvery == 0) {
      println(s"Sent $iteration messages!")
      val sleeping = rand.nextInt(incrementTimeEvery * 60)
      println(s"Sleeping for $sleeping ms")
      Thread sleep sleeping
    }
  }

  fileWriter.close()


  def readResourceFileLineByLine(resource: String): Array[String] = {
    Source.fromInputStream(getClass.getResourceAsStream(resource)).getLines().toArray
  }

  def getRandomItem[T](array: Array[T]): T = {
    array(rand.nextInt(array.length - 1))
  }
}
