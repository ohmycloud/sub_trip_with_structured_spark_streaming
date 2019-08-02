package com.gac.x9e.core.impl

import com.gac.x9e.core.EnAdapter
import org.apache.spark.sql.{DataFrame, SparkSession}

object EnAdapterImpl extends EnAdapter {
  override def extract(spark: SparkSession, df: DataFrame): DataFrame = ???
}
