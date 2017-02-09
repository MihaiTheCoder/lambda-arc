import org.apache.spark.sql.Row
import org.apache.spark.streaming.kafka.OffsetRange

/**
  * Created by Mihai.Petrutiu on 1/31/2017.
  */
package object domain {
  case class Activity(timestamp_hour: Long,
                      referrer: String,
                      action: String,
                      prevPage: String,
                      page: String,
                      visitor: String,
                      product: String,
                      inputProps: Map[String, String]= Map())

  object Activity {
    val timestamp_hour = "timestamp_hour"
    val referrer = "referrer"
    val action = "action"
    val prevPage = "prevPage"
    val page = "page"
    val visitor = "visitor"
    val product = "product"
    val topic = "topic"
    val kafkaPartition = "kafkaPartition"
    val fromOffset = "fromOffset"
    val untilOffset = "untilOffset"
  }

  case class ActivityByProduct (product: String,
                                timestamp_hour: Long,
                                purchase_count: Long,
                                add_to_cart_count: Long,
                                page_view_count: Long)

  case class VisitorByProduct(product: String, timestamp_hour: Long, unique_visitors: Long)

  object ActivityFactory {

    private val MS_IN_HOUR = 1000 * 60 * 60

    def getActivity(line: String, inputProps:Map[String, String] = Map()): Option[Activity] = {
      val splitLine = line.split("\\t")
      if (splitLine.length == 7) {
        val timestamp_hour = splitLine(0).toLong / MS_IN_HOUR * MS_IN_HOUR
        return Some(Activity(timestamp_hour, splitLine(1), splitLine(2), splitLine(3), splitLine(4), splitLine(5), splitLine(6), inputProps))
      }
      else {
        return None
      }
    }

    def getActivity(line: String, offsetRange: OffsetRange): Option[Activity] = {
      val map = Map(
        Activity.topic -> offsetRange.topic,
        Activity.kafkaPartition -> offsetRange.partition.toString,
        Activity.fromOffset -> offsetRange.fromOffset.toString,
        Activity.untilOffset -> offsetRange.untilOffset.toString
      )

      val activity = getActivity(line, map)

      activity
    }

    def getActivityWithOffserRangeColumns: List[String] = {
      List(Activity.timestamp_hour, Activity.referrer, Activity.action, Activity.prevPage,
        Activity.page, Activity.visitor, Activity.product,
        s"inputProps.${Activity.topic} as ${Activity.topic}",
        s"inputProps.${Activity.kafkaPartition} as ${Activity.kafkaPartition}",
        s"inputProps.${Activity.fromOffset} as ${Activity.fromOffset}",
        s"inputProps.${Activity.untilOffset} as ${Activity.untilOffset}")
    }
  }

  object ActivityByProductFactory {
    def getActivityByProduct(row: Row) : ActivityByProduct = {
      ActivityByProduct(row.getString(0), row.getLong(1), row.getLong(2), row.getLong(3), row.getLong(4))
    }

  }
}
