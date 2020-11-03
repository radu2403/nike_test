package ingest

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import pipeline.{DataLink, Link}

case class Ingestion(link: Link) {
  def addWeeksOfYear(tableName: String)(implicit spark: SparkSession): Map[String, Link] = {
    import spark.sqlContext.implicits._
    Map(
      tableName -> link,
      "weeks_of_year" -> new DataLink((1 to 3).toDF("weekofyear"))
    )
  }
}
