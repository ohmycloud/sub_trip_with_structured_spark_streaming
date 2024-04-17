package com.gac.x9e

import com.alibaba.fastjson.JSON
import com.gac.x9e.conf.SocketConfiguration
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.LoggerFactory
import scala.collection.mutable.ArrayBuffer

/**
 *  基于处理时间，用 `flatMapGroupsWithState` 统计每个分组的 PV，并手动维护状态
 */
object FlatMapGroupsWithState extends App {

  lazy val logger = LoggerFactory.getLogger(this.getClass)
  private val datetimeFormat = "yyyy-MM-dd HH:mm:ss"
  private val timeZoneFormat = "GMT+8"
  private val watermarkDuration = "5 minutes"
  private val timeoutDuration = 10 * 1000L

  case class UserEvent(
                        eventTime: Timestamp,
                        eventType: String,
                        userId: String
                      )

  case class UserSession(
                          userId: String,
                          eventType: String,
                          eventCount: Long
                        )

  /**
   * 带时区的时间转换为 Timestamp
   *
   * @param dateTime 数据时间
   * @param dataTimeFormat 时间格式
   * @param dataTimeZone 时间区间
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
      case ex: Exception => logger.error("时间转换异常..." + dateTime, ex)
    }
    output
  }

  private def mappingFunction(groupKey: String,
                              source: Iterator[UserEvent],
                              userState: GroupState[UserSession]
                             ): Iterator[UserSession] = {
    println("当前组对应的 Key: " + groupKey)
    println("当前 Watermark: " + userState.getCurrentWatermarkMs())
    println("当前组的状态是否存在: " + userState.exists)
    println("当前组的状态是否过期: " + userState.hasTimedOut)

    val key = groupKey.split(",")
    var totalValue = 0L

    // 当前组状态已过期，则清除状态
    if (userState.hasTimedOut) {
      println("清除状态...")
      userState.remove()
      // 当前组状态已存在，则根据需要处理
    } else if (userState.exists) {
      println("增量聚合....")
      // 历史值: 从状态中获取
      val historyValue = userState.get.eventCount
      // 当前值: 从当前组的新数据计算得到
      val currentValue = source.size
      // 总值=历史+当前
      totalValue = historyValue + currentValue

      // 更新状态
      val newState = UserSession(key(0), key(1), totalValue )
      userState.update(newState)

      // 设置状态超时时间
      userState.setTimeoutTimestamp(userState.getCurrentWatermarkMs() + timeoutDuration)
      // 当前组状态不存在，则初始化状态
    } else {
      println("初始化状态...")
      totalValue = source.size
      val initialState = UserSession(key(0), key(1), totalValue * 1L )
      userState.update(initialState)
      userState.setTimeoutTimestamp(userState.getCurrentWatermarkMs() + timeoutDuration)
    }

    val output = ArrayBuffer[UserSession]()
    if (userState.getOption.nonEmpty) {
      output.append(userState.get)
    }

    output.iterator
  }

  val spark = SparkSession
    .builder()
    .master("local[3]")
    .appName(this.getClass.getSimpleName.replace("$", ""))
    .getOrCreate()

  val socketConf = new SocketConfiguration
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val inputDs = spark
    .readStream
    .format("socket")
    .option("host", socketConf.host)
    .option("port", socketConf.port)
    .load()

  private val adapterDs: Dataset[UserEvent] = inputDs.as[String]
    .map { line =>
      try {
        val data = JSON.parseObject(line)
        val eventTime = timezoneToTimestamp(data.getString("eventTime"), datetimeFormat, timeZoneFormat)
        val eventType = data.getString("eventType")
        val userId = data.getString("userId")

        // 将每行数据转换为 SourceData 数据源
        UserEvent(eventTime, eventType, userId)
      } catch {
        case e: Exception =>
          println(s"[$line] can't be parsed")
          UserEvent(new Timestamp(0L), "", "")
      }
    }.filter(x => x.userId.nonEmpty)

  private val resultDs = adapterDs
    .withWatermark("eventTime", watermarkDuration)
    .groupByKey(x => s"${x.userId},${x.eventType}")
    .flatMapGroupsWithState(
      outputMode = OutputMode.Update(),
      timeoutConf = GroupStateTimeout.EventTimeTimeout()
    )(func = mappingFunction)

  private val query = resultDs
    .writeStream
    .format("console")
    .option("truncate", "false")
    .outputMode(OutputMode.Update())
    .start()

  query.awaitTermination()
}

