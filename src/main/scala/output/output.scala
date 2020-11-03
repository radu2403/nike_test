package output

import org.apache.spark.sql.DataFrame
import pipeline.{Link, TransformationLink}

case class output(link: Link) {
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