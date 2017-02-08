import com.twitter.algebird.{HLL, HyperLogLogMonoid}
import domain.ActivityByProduct
import org.apache.spark.streaming.State

/**
  * Created by Mihai.Petrutiu on 2/7/2017.
  */
package object functions {

  def mapVisitorsStateFunc = (k: (String, Long), v: Option[HLL], state: State[HLL]) => {
    val currentVisitorHLL = state.getOption().getOrElse(new HyperLogLogMonoid(12).zero)
    val newVisitorHLL = v match {
      case Some(visitorHLL) => currentVisitorHLL + visitorHLL
      case None => currentVisitorHLL
    }
    state.update(newVisitorHLL)
    val output = newVisitorHLL.approximateSize.estimate
    output

  }

  def mapActivityStateFunc = (k: (String, Long), v: Option[ActivityByProduct], state: State[(Long, Long, Long)]) => {
    var (purchase_count, add_to_cart_count, page_view_count) =state.getOption().getOrElse((0L, 0L, 0L))

    val newVal = v match {
      case Some(a: ActivityByProduct) => (a.purchase_count, a.add_to_cart_count, a.page_view_count)
      case _ => (0L, 0L, 0L)
    }

    purchase_count += newVal._1
    add_to_cart_count += newVal._2
    page_view_count += newVal._3

    state.update((purchase_count, add_to_cart_count, page_view_count))

    val underExposed = {
      if (purchase_count == 0)
        0
      else
        page_view_count / purchase_count
    }
    underExposed
  }

  def updateStateByKey(newItemsPerKey: Seq[ActivityByProduct], currentState: Option[(Long, Long, Long, Long)]):
  Option[(Long, Long, Long, Long)] = {
    var (prevTimeStamp, purchase_count, add_to_cart_count, page_view_count) = currentState.getOrElse((System.currentTimeMillis(), 0L, 0L, 0L))
    var result: Option[(Long, Long, Long, Long)] = null

    if (newItemsPerKey.isEmpty) {
      if (System.currentTimeMillis() - prevTimeStamp > 30000 + 4000)
        result = None

      else
        result = Some((prevTimeStamp, purchase_count, add_to_cart_count, page_view_count))
    } else {
      newItemsPerKey.foreach(a => {
        purchase_count += a.purchase_count
        add_to_cart_count += a.add_to_cart_count
        page_view_count += a.page_view_count
      })
      result = Some((System.currentTimeMillis(), purchase_count, add_to_cart_count, page_view_count))
    }

    result
  }
}
