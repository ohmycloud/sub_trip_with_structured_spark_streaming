package com.gac.x9e.core.impl

import java.sql.Timestamp
import com.alibaba.fastjson.JSON
import com.gac.x9e.core.Adapter
import com.gac.x9e.model.SourceData
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * fake-stream --host='0.0.0.0' --port=3333 --vin='LSJA0000000000091'  --last_meter=0 --sleeping=10 --rate=1
 * {'vin':'LSJA0000000000091','createTime':1564717333000,'mileage':0}
 * {'vin':'LSJA0000000000091','createTime':1564717334000,'mileage':1}
 * {'vin':'LSJA0000000000091','createTime':1564717335000,'mileage':2}
 * {'vin':'LSJA0000000000091','createTime':1564717336000,'mileage':3}
 * {'vin':'LSJA0000000000091','createTime':1564717337000,'mileage':4}
 * {'vin':'LSJA0000000000091','createTime':1564717338000,'mileage':5}
 * {'vin':'LSJA0000000000091','createTime':1564717339000,'mileage':6}
 * {'vin':'LSJA0000000000091','createTime':1564717340000,'mileage':7}
 * {'vin':'LSJA0000000000091','createTime':1564717341000,'mileage':8}
 * {'vin':'LSJA0000000000091','createTime':1564717342000,'mileage':9}
 * {'vin':'LSJA0000000000091','createTime':1564717363000,'mileage':10}
 * ....
 * 数据适配： val data = JSON.parseObject("{\"vin\":\"LSJA0000000000091\",\"createTime\":1564705982000,\"mileage\":5}")
 * data.getTimestamp("createTime")： java.sql.Timestamp = 2019-08-02 08:33:02.0
 */
object AdapterImpl extends Adapter {
  override def extract(spark: SparkSession, df: DataFrame): Dataset[SourceData] = {
    import spark.implicits._
    df.as[String]
      .map{ line =>

        val data       = JSON.parseObject(line)
        val vin        = data.getString("vin")
        val createTime = new Timestamp( data.getLong("createTime"))
        val mileage    = data.getLong("mileage")

        // 将每行数据转换为 SourceData 数据源
        SourceData(vin, createTime, mileage)
      }
  }
}
