package com.gac.x9e.pipeline

import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataSource[M] {
  def stream(): DataFrame
}

trait WrapperSparkSession[M] {
  def session(): SparkSession
}