package clickstream

import java.io.FileWriter
import java.util.Properties

import config.Settings
import org.apache.commons.io.FileUtils

import scala.io.Source
import scala.util.Random
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}

/**
  * Created by Mihai.Petrutiu on 1/31/2017.
  */
object LogProducer extends App {
  val webLog = Settings.WebLogGen

  val products = readResourceFileLineByLine("/products.csv")
  val referrers = readResourceFileLineByLine("/referrers.csv")
  val visitors = (0 to webLog.visitors).map("Visitor-" + _).toArray
  val pages = (0 to webLog.pages).map("Page-" + _).toArray
  val topic = webLog.topic

  val rand = new Random()

  val props = new Properties()
  val STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer"
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, webLog.bootstrapServersConfig)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER)
  props.put(ProducerConfig.ACKS_CONFIG, "all")
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "WebLogProducer")

  val kafkaProducer = new KafkaProducer[Nothing, String](props)
  println(kafkaProducer.partitionsFor(topic))

  for (fileCount <- 1 to webLog.numberOfFiles) {

    //val fileWriter = new FileWriter(filePath, true)

    // introduce some randomness to time increments for demo purposes
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
      val producerRecord = new ProducerRecord(topic, line)
      kafkaProducer.send(producerRecord)
      //fileWriter.write(line)

      if (iteration % incrementTimeEvery == 0) {
        println(s"Sent $iteration messages!")
        val sleeping = rand.nextInt(incrementTimeEvery * 60)
        println(s"Sleeping for $sleeping ms")
        Thread sleep sleeping
      }
    }

    //fileWriter.close()
    //val outputFile = FileUtils.getFile(s"${destPath}data_$timestamp")
    //println(s"Moving produced data to $outputFile")
    //FileUtils.moveFile(FileUtils.getFile(filePath), outputFile)
    val sleeping = 5000
    println(s"sleeping for $sleeping ms")
  }

  kafkaProducer.close()


  def readResourceFileLineByLine(resource: String): Array[String] = {
    Source.fromInputStream(getClass.getResourceAsStream(resource)).getLines().toArray
  }

  def getRandomItem[T](array: Array[T]): T = {
    array(rand.nextInt(array.length - 1))
  }
}
