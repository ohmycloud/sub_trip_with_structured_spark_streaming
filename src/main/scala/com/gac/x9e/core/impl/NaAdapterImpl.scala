package com.gac.x9e.core.impl

import com.alibaba.fastjson.JSON
import com.gac.x9e.core.NaAdapter
import com.gac.x9e.model.SourceData
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object NaAdapterImpl extends NaAdapter {
  override def extract(spark: SparkSession, df: DataFrame): Dataset[SourceData] = {
    import spark.implicits._
    df.as[String]
      .map{ case line =>

        val data       = JSON.parseObject(line)
        val vin        = data.getString("vin")
        val createTime = data.getLong("createTime")
        val mileage    = data.getLong("mileage")

        // 将每行数据转换为 SourceData 数据源
        SourceData(vin, createTime, mileage)
      }
  }
}
