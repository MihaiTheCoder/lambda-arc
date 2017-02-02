package batch

import config.Settings
import domain._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Mihai.Petrutiu on 1/31/2017.
  */
object BatchJobV1 {
  val tsvFile = "E:\\VagrantBoxes\\spark-kafka-cassandra-applying-lambda-architecture\\vagrant\\data.tsv"
  def main(args: Array[String]): Unit = {

    println("start")
    val y =util.Properties.versionString
    val x = Settings.WebLogGen.records
    //get spark configuration
    val conf = new SparkConf()
      .setAppName("Lambda with Spark")

    //If we are running from IDE
    if(Settings.WebLogGen.isDebug) {
      println("debug run")
      System.setProperty("hadoop.home.dir", "E:\\Scala\\WinUtils\\hadoop-common-2.2.0-bin-master")
      conf.setMaster("local[*]")
    }

    //setup spark context
    val sc = new SparkContext(conf)



    //initialize input RDD
    val sourceFile = tsvFile
    val input = sc.textFile(sourceFile)

    //spark action results in job execution
    //input.foreach(println)

    //createRDD(input)

    createDF(sc, input)
  }

  private def createDF(sc: SparkContext, input: RDD[String]) = {
    implicit val sqlContext = new SQLContext(sc)

    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    val inputDF = input
      .flatMap(line => ActivityFactory.getActivity(line))
      .toDF().cache()

    val df = inputDF.select(
      add_months(from_unixtime(inputDF("timestamp_hour") / 1000), 1).as("timestamp_month"),
      inputDF("referrer"), inputDF("action"), inputDF("prevPage"), inputDF("page"), inputDF("visitor"), inputDF("product")
    )

    df.registerTempTable("activity")

    val visitorByProduct = sqlContext.sql(
      """SELECT product, timestamp_month, COUNT(DISTINCT visitor) as unique_visitors
        |FROM activity GROUP BY product, timestamp_month
      """.stripMargin)

    visitorByProduct.printSchema()

    sqlContext.udf.register("UnderExposed", (pageViewCount: Long, purchaseCount: Long) => if (purchaseCount == 0) 0 else pageViewCount / purchaseCount)
    val activityByProduct = sqlContext.sql(
      """SELECT
                                            product,
                                            timestamp_month,
                                            sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
                                            sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                                            sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
                                            from activity
                                            group by product, timestamp_month """)

    activityByProduct.registerTempTable("activityByProduct")

    val query = "SELECT product, timestamp_month, UnderExposed(page_view_count, purchase_count) as negative_exposure " +
      "from activityByProduct order by negative_exposure DESC limit 5"
    val underExposed = sqlContext.sql(query)


    visitorByProduct.foreach(println)
    activityByProduct.foreach(println)
    underExposed.foreach(println)
  }

  private def createRDD(input: RDD[String]) = {
    val inputRDD: RDD[Activity] = input.flatMap(line => ActivityFactory.getActivity(line))

    val keyedByProductPerHour = inputRDD.keyBy(a => (a.product, a.timestamp_hour)).cache()

    val visitorsByProduct = keyedByProductPerHour
      .mapValues(a => a.visitor)
      .distinct()
      .countByKey()

    val activityByProduct = keyedByProductPerHour
      .mapValues { a =>
        a.action match {
          case "purchase" => (1, 0, 0)
          case "add_to_cart" => (0, 1, 0)
          case "page_view" => (0, 0, 1)
        }
      }.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))


    visitorsByProduct.foreach(println)
    activityByProduct.foreach(println)
  }
}
