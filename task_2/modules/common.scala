package com.example.etl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger

object Common {
  val logger = Logger.getLogger(getClass.getName)

  def cleanColumnNames(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df) { (tempDf, colName) =>
      tempDf.withColumnRenamed(colName, colName.trim.replaceAll("\\s+", "_").toLowerCase)
    }
  }

  def addMetadata(df: DataFrame, fileName: String): DataFrame = {
    df.withColumn("raw_file_name", lit(fileName))
      .withColumn("load_timestamp", current_timestamp())
  }

  def upsertToDelta(df: DataFrame, path: String, tableName: String, mergeCondition: String)(implicit spark: SparkSession): Unit = {
    import io.delta.tables._

    val deltaTable = DeltaTable.forPath(spark, path)
    deltaTable.as("t")
      .merge(
        df.as("s"),
        mergeCondition
      )
      .whenMatched().updateAll()
      .whenNotMatched().insertAll()
      .execute()

    df.write.format("delta").mode("append").saveAsTable(tableName)
  }
}
