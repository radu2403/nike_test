package pipeline
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._


class FactoryPipeline(private val spark: SparkSession) {
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

  case class Ingestion(link: Link) {
    import spark.sqlContext.implicits._

    def addWeeksOfYear(tableName: String): Map[String, Link] = {
      Map(
        tableName -> link,
        "weeks_of_year" -> new DataLink((1 to 3).toDF("weekofyear"))
      )
    }
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

  case class JoinMap(m: Map[String, Link]) {
    def getJoin(sqlQuery: String) = {

      new JoinLink((transMap) => {
                                    transMap.map(t => t._1-> t._2.createOrReplaceTempView(t._1))
                                    spark.sqlContext.sql(sqlQuery)
                                  },
                    m
      )
    }
  }

  implicit def join(map: Map[String, Link]) = JoinMap(map)


//  TRANSFORMATIONS
  val createYearFn = (year: Int) => "RY" + (year%100).toString
  val createUniqueIdFn = (year: String, channel: String, division: String, gender: String, category: String) => Seq(year, channel, division, gender, category).mkString("_")

  case class Transform(link: Link) {
    def addYear(add: Int => String) = {

      new TransformationLink((df: DataFrame)=>{
                                                  val createYearFnUdf = udf(add)
                                                  df.withColumn("year", createYearFnUdf(df("datecalendaryear")))
                                                },
        link)

    }

    def addUniqueId(addId: (String, String, String, String, String) => String) = {

      new TransformationLink((df: DataFrame) => {
                                                  val createUniqueIdFnUdf = udf(createUniqueIdFn)
                                                  df.withColumn("uniqueId", createUniqueIdFnUdf(
                                                                                                          df("year"),
                                                                                                          df("channel"),
                                                                                                          df("division"),
                                                                                                          df("gender"),
                                                                                                          df("category")
                                                                                                        )
                                                  )
                                                },
        link)
    }


    def salesPerWeek() = {
      new TransformationLink(
        (df: DataFrame) => {
                              df.groupBy(df("uniqueId"),df("channel"), df("division"), df("gender"), df("category"))
                                .agg(collect_list(map(df("weekofyear"),df("netSales"))).as("netSales"),
                                     collect_list(map(df("weekofyear"), df("salesUnits"))).as("salesUnits")
                                    )
                            },
        link
      )
    }

  }

  implicit def trans(link: Link) = Transform(link)


//  OUTPUT
  case class Output(link: Link) {
  def sinkToJsonPartitions() = {
      new TransformationLink(
        (df: DataFrame) => {
          val rows = df.count()
          df.repartition(rows.toInt).write.mode("overwrite").json("./json/")
          df
        },
      link)
  }

  }

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

