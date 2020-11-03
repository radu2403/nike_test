package transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{collect_list, map, udf}
import pipeline.{Link, TransformationLink}

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
      val createUniqueIdFnUdf = udf(addId)
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