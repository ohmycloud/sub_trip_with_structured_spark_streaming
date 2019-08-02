package com.gac.x9e.pipeline

import org.apache.spark.sql.{DataFrame, SparkSession}

trait NaSource[M] {
  def stream(): DataFrame
}

trait EnSource[M] {
  def stream(): DataFrame
}

trait NaSparkSession[M] {
  def session(): SparkSession
}

trait EnSparkSession[M] {
  def session(): SparkSession
}
