package pipeline
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import output.Output
import _root_.transform.{JoinMap, Transform}
import ingest.Ingestion


class FactoryPipeline(implicit private val spark: SparkSession) {
  private val CALENDAR_PATH = "./data/calendar.csv"
  private val PRODUCT_PATH = "./data/product.csv"
  private val SALES_PATH = "./data/sales.csv"
  private val STORE_PATH = "./data/store.csv"


//  INGESTION
  private def getIngestionChain(): Map[String, Link] = {
    val sales = spark.read.options(Map("header" -> "true")).csv(SALES_PATH)
    val product = spark.read.options(Map("header"->"true")).csv(PRODUCT_PATH)
    val calendar = spark.read.options(Map("header"->"true")).csv(CALENDAR_PATH)
    val store = spark.read.options(Map("header"->"true")).csv(STORE_PATH)

    Map("sales" -> new DataLink(sales),
        "product" -> new DataLink(product),
        "calendar" -> new DataLink(calendar),
        "store" -> new DataLink(store)
    )
  }


  implicit def ingest(link: Link) = Ingestion(link)



//  JOIN
  def weekOfYearSqlJoin =
    """
      |SELECT
      |                   t.uniqueId,
      |                   CONCAT("W", CAST(w.weekofyear AS STRING)) AS weekofyear,
      |                   channel,
      |                   division,
      |                   gender,
      |                   category,
      |                   IF(t.weeknumberofseason = w.weekofyear, netSales, 0) AS netSales,
      |                   IF(t.weeknumberofseason = w.weekofyear, salesUnits, 0) AS salesUnits
      |
      |            FROM total_agg_sales t, weeks_of_year w
      |""".stripMargin
  def joinSql = """SELECT p.division,
                  |                   p.gender,
                  |                   p.category,
                  |
                  |                   st.channel,
                  |                   st.country,
                  |
                  |                   c.datecalendaryear,
                  |                   c.weeknumberofseason,
                  |
                  |                   s.netSales,
                  |                   s.salesUnits
                  |            FROM sales s
                  |
                  |            INNER JOIN product p
                  |            ON s.productId = p.productid
                  |
                  |            INNER JOIN store st
                  |            ON s.storeId = st.storeid
                  |
                  |            INNER JOIN calendar c
                  |            ON s.dateId = c.datekey""".stripMargin



  implicit def join(map: Map[String, Link]) = JoinMap(map)


//  TRANSFORMATIONS
  val createYearFn = (year: Int) => "RY" + (year%100).toString
  val createUniqueIdFn = (year: String, channel: String, division: String, gender: String, category: String) => Seq(year, channel, division, gender, category).mkString("_")
  implicit def trans(link: Link) = Transform(link)


//  OUTPUT
  implicit def writeOut(link: Link) = Output(link)


//  The pipeline creation
  def getPipeline(): Link = {
    val pipeline = getIngestionChain()
                            .getJoin(joinSql)
                            .addYear(createYearFn)
                            .addUniqueId(createUniqueIdFn)
                            .addWeeksOfYear("total_agg_sales")
                            .getJoin(weekOfYearSqlJoin)
                            .salesPerWeek()
                            .sinkToJsonPartitions()


    pipeline
  }

}

