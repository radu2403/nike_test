package transform

import org.apache.spark.sql.SparkSession
import pipeline.{JoinLink, Link}

case class JoinMap(m: Map[String, Link]) {
  def getJoin(sqlQuery: String)(implicit spark: SparkSession) = {

    new JoinLink((transMap) => {
      transMap.map(t => t._1-> t._2.createOrReplaceTempView(t._1))
      spark.sqlContext.sql(sqlQuery)
    },
      m
    )
  }
}
