import org.apache.spark.sql.SparkSession
import com.example.etl.Common._

object Bronze {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("BronzeLayer").getOrCreate()

    val paths = Map(
      "purchase" -> "s3://tendo-de-test/purchase.csv",
      "avocado" -> "s3://tendo-de-test/avocado.csv",
      "consumer" -> "s3://tendo-de-test/consumer.csv",
      "fertilizer" -> "s3://tendo-de-test/fertilizer.csv"
    )

    val bronzePath = "/mnt/bronze"
    val tableName = "tendo_de.bronze"

    paths.foreach { case (key, path) =>
      try {
        val df = spark.read.format("csv").option("header", "true").load(path)
        val cleanedDf = cleanColumnNames(df)
        val finalDf = addMetadata(cleanedDf, path)
        finalDf.write.format("delta").mode("append").save(s"$bronzePath/$key")
        finalDf.write.format("delta").mode("append").saveAsTable(s"$tableName.$key")
        logger.info(s"Data from $path ingested to Bronze successfully.")
      } catch {
        case e: Exception =>
          logger.error(s"Error ingesting data from $path to Bronze: ${e.getMessage}")
          throw e
      }
    }

    spark.stop()
  }
}