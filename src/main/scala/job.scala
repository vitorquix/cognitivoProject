import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.json4s._
import org.json4s.jackson.JsonMethods._

object job {

  def main(args: Array[String]): Unit = {

    //Default configs
    val spark: SparkSession = SparkSession
      .builder()
      .appName("COGNITIVO_ANALISE")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    implicit val formats = DefaultFormats

    //Mapper - Data Types
    val mapper = spark.read.
      option("multiline", true).
      json("/home/vitorsampaio/IdeaProjects/cognitivoProject/config/")

    //Raw Data
    val load_raw = spark.read.
        option("header", true).
        csv("/home/vitorsampaio/IdeaProjects/cognitivoProject/data/input/users/")

    //Rank
    val load_rank = load_raw.withColumn("rank", row_number.over(Window.partitionBy("id").orderBy(desc("update_date")))).
      filter($"rank"==="1").
      sort(desc("id")).
      drop($"rank")

    //Normalize
    val mapper_columns = parse(mapper.toJSON.collect()(0)).extract[Map[String,Any]]

    val load_final = load_rank.
      columns.
      foldLeft(load_rank) { (memoDF, colName) =>
        if (mapper_columns.contains(colName)) memoDF.withColumn(colName, trim(lower(memoDF.col(colName).cast(mapper_columns(colName).toString))))
        else memoDF.withColumn(colName, trim(lower(col(colName))))
      }

    //Write
    load_final.
      coalesce(1).
      write.
      mode("overwrite").
      parquet("/home/vitorsampaio/IdeaProjects/cognitivoProject/data/output/persist")

  }
}
