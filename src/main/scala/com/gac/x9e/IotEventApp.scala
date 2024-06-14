package com.gac.x9e

import com.alibaba.fastjson.JSON
import com.gac.x9e.conf.{SocketConfiguration, SparkConfiguration}
import com.gac.x9e.core.impl.IotEventImpl
import com.gac.x9e.model.IotData
import com.gac.x9e.util.Xutil
import org.apache.spark.sql.functions.{col, struct, to_json}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}

object IotEventApp extends App {
  private val datetimeFormat = "yyyy-MM-dd HH:mm:ss"
  private val timeZoneFormat = "GMT+8"
  
  val sparkConf = new SparkConfiguration
  val socketConf = new SocketConfiguration

  val spark = Xutil.iotSparkSession(sparkConf)
  spark.sparkContext.setLogLevel(sparkConf.logLevel)

  /**
   * 带时区的时间转换为 Timestamp
   *
   * @param dateTime       数据时间
   * @param dataTimeFormat 时间格式
   * @param dataTimeZone   时间区间
   * @return
   */
  private def timezoneToTimestamp(dateTime: String, dataTimeFormat: String, dataTimeZone: String): Timestamp = {
    var output: Timestamp = null
    try {
      if (dateTime != null) {
        val format = DateTimeFormatter.ofPattern(dataTimeFormat)
        val eventTime = LocalDateTime.parse(dateTime, format).atZone(ZoneId.of(dataTimeZone));
        output = new Timestamp(eventTime.toInstant.toEpochMilli)
      }
    } catch {
      case ex: Exception => println("时间转换异常..." + dateTime, ex)
    }
    output
  }

  import spark.implicits._

  val inputDs = spark
    .readStream
    .format("socket")
    .option("host", socketConf.host)
    .option("port", socketConf.port)
    .load()

  private val ds: Dataset[IotData] = inputDs.as[String]
    .map { line =>
      try {
        val data = JSON.parseObject(line)
        val dataTime = timezoneToTimestamp(data.getString("data_time"), datetimeFormat, timeZoneFormat)
        val stationId = data.getString("station_id")

        // 将每行数据转换为 SourceData 数据源
        IotData(stationId, dataTime)
      } catch {
        case e: Exception =>
          println(s"[$line] can't be parsed")
          IotData("", new Timestamp(0L))
      }
    }.filter(x => x.stationId.nonEmpty)
    .as[IotData]

  val adapterDs =  IotEventImpl.extract(spark, ds)

  adapterDs
    .select($"stationId" as "key", to_json(struct(adapterDs.columns.map(col): _*)) as "value")
    .writeStream
    .format("console")
    .queryName("iot-status-query")
    .option("truncate", "false")
    .outputMode(OutputMode.Update())
    .trigger(Trigger.ProcessingTime(sparkConf.sparkInterval))
    .start()

  spark.streams.awaitAnyTermination()
}
