package pipeline
import org.apache.spark.sql.DataFrame

trait Link {
  def execute(): DataFrame
}

class DataLink(private val df: DataFrame) extends Link {
  override def execute(): DataFrame = df
}


class TransformationLink(private val dfTransformation: DataFrame => DataFrame, nextLink: Link) extends Link {
  val df: DataFrame = dfTransformation(nextLink.execute())

  override def execute = df
}

class JoinLink(private val dfTransformation: Map[String, DataFrame] => DataFrame, nextLinkMap: Map[String, Link]) extends Link {
  val df: DataFrame = dfTransformation(nextLinkMap.map(m => m._1 -> m._2.execute()))

  override def execute(): DataFrame = df
}