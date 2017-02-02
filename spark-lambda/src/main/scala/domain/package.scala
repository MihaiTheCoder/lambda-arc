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

  object ActivityFactory {

    private val MS_IN_HOUR = 1000*60*60
    def getActivity(line: String) : Option[Activity] = {
      val splitLine = line.split("\\t")
      if (splitLine.length == 7) {
        val timestamp_hour = splitLine(0).toLong / MS_IN_HOUR * MS_IN_HOUR
        return Some(Activity(timestamp_hour, splitLine(1), splitLine(2), splitLine(3), splitLine(4), splitLine(5), splitLine(6)))
      }
      else {
        return None
      }
    }
  }
}
