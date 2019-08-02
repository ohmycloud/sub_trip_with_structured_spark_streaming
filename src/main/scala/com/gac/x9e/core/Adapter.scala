package com.gac.x9e.core

import com.gac.x9e.model.{SourceData, TripUpdate}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

// 企标数据适配
trait EnAdapter {
  def extract(spark: SparkSession, df: DataFrame): DataFrame
}

// 国标数据适配
trait NaAdapter {
  def extract(spark: SparkSession, df: DataFrame): Dataset[SourceData]
}

trait NaSubTrip {
  def extract(spark: SparkSession, ds: Dataset[SourceData]): Dataset[TripUpdate]
}