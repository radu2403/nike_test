import org.apache.spark.sql.SparkSession
import pipeline.FactoryPipeline

object Program {


  def getSparkSession(): SparkSession = {
    SparkSession.builder.config("spark.master", "local").appName("Simple Application").getOrCreate()
  }
  def main(args: Array[String]) = {
    val spark = getSparkSession()
    val pipelineFactory = new FactoryPipeline(spark)
    val pipeline = pipelineFactory.getPipeline()


    print(pipeline.execute().show())

    spark.stop()

  }
}