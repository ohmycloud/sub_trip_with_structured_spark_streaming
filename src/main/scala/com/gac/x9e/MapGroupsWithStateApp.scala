package com.gac.x9e

import com.alibaba.fastjson.JSON
import com.gac.x9e.conf.SocketConfiguration

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.slf4j.LoggerFactory


/**
 *   基于事件时间, 用 `mapGroupsWithState` 统计每个分组的 PV，并手动维护状态
 */
object MapGroupsWithStateApp extends App {

  lazy val logger = LoggerFactory.getLogger(this.getClass)

  private val datetimeFormat = "yyyy-MM-dd HH:mm:ss"
  private val timeZoneFormat = "GMT+8"
  private val watermarkDuration = "5 minutes"

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
      case ex: Exception => logger.error("时间转换异常..." + dateTime, ex)
    }
    output
  }

  val spark = SparkSession
    .builder().master("local[2]")
    .appName(this.getClass.getSimpleName.replace("$", ""))
    .getOrCreate()

  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

//  implicit val userEventEncoder: Encoder[UserEvent] = Encoders.kryo[UserEvent]
  implicit val userSessionEncoder: Encoder[Option[UserSession]] = Encoders.kryo[Option[UserSession]]

  // 测试数据，如下:
  // {"eventTime": "2016-01-01 10:02:00" ,"eventType": "click" ,"userId":"1"}
  val socketConf = new SocketConfiguration

  private val inputDs: Dataset[String] = spark
    .readStream
    .format("socket")
    .option("host", socketConf.host)
    .option("port", socketConf.port)
    .load()
    .as[String]

  private val adapterDs: Dataset[UserEvent] = inputDs.map { line =>
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

  private def mappingFunction(
                               groupKey: String,
                               source: Iterator[UserEvent],
                               userState: GroupState[UserSession]
                             ): Option[UserSession] = {
    println("当前组对应的Key: " + groupKey)
    println("当前Watermark: " + userState.getCurrentWatermarkMs())
    println("当前组的状态是否存在: " + userState.exists)
    println("当前组的状态是否过期: " + userState.hasTimedOut)

    var totalValue = 0L
    val key = groupKey.split(",")

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
      val newState = UserSession(key(0), key(1), totalValue)
      userState.update(newState)

      // 事件时间模式下，不需要设置超时时间，会根据 Watermark 机制自动超时
      // 处理时间模式下，可设置个超时时间，根据超时时间清理状态，避免状态无限增加
      // groupState.setTimeoutDuration(1 * 10 * 1000)
      // 当前组状态不存在，则初始化状态
    } else {
      println("初始化状态...")
      totalValue = source.size
      val initialState = UserSession(key(0), key(1), totalValue * 1L)
      userState.update(initialState)
    }

    if (userState.getOption.nonEmpty) {
      Some(UserSession(key(0), key(1), userState.get.eventCount))
    } else {
      None
    }
  }

  private val resultDs: Dataset[UserSession] = adapterDs
    .withWatermark("eventTime", watermarkDuration)
    .groupByKey(x => s"${x.userId},${x.eventType}")
    .mapGroupsWithState(GroupStateTimeout.EventTimeTimeout())(mappingFunction)
    .flatMap(userSession => userSession)

  resultDs
    .writeStream
    .format("console")
    .option("truncate", "false")
    .outputMode(OutputMode.Update())
    .start()

  spark.streams.awaitAnyTermination()
}

