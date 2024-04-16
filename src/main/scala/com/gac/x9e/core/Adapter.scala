package com.gac.x9e.core

import com.gac.x9e.model.{SourceData, TripSession}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

// 数据适配
trait Adapter {
  def extract(spark: SparkSession, df: DataFrame): Dataset[SourceData]
}

trait SubTrip {
  def extract(spark: SparkSession, ds: Dataset[SourceData]): Dataset[TripSession]
}