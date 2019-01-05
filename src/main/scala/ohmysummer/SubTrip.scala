package ohmysummer

import com.alibaba.fastjson.JSON
import ohmysummer.conf.SocketConfiguration
import ohmysummer.model.{SourceData, TripInfo, TripUpdate}
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}
import org.apache.spark.streaming.Duration


/**
  * 使用 Structured Streaming 进行行程划分
  * 问题：一个 sourceData 里面, 某 vin 会有俩个或俩个以上的行程吗？
  * 超时时间为 15 分钟, 超时则返回 TripUpdate, 并将 status 置为 2
  * 俩条数据之间的 createTime 之差如果大于 5 分钟, 则结束上一个行程, 并开始下一个行程
  * 数据每 5 秒一个微批，可知一个微批里面, 某 vin 不会出现俩个以上的行程!
  *
  * 我们测试当然不能等那么久, ProcessingTime 即微批时间我们设置 1s 一次
  * 俩条数据之间的 createTime 之差如果大于 1 分钟, 则结束上一个行程, 并开始下一个新的行程
  * 超时时间设置为 3 分钟
  */
object SubTrip {

  def main(args: Array[String]): Unit = {

    val socketConf = new SocketConfiguration
    val host = socketConf.host
    val port = socketConf.port

    val spark: SparkSession = SparkSession.builder
      .master("local[2]")
      .appName("Stateful Structured Streaming")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val ds: Dataset[String] = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()
      .as[String]

    val events = ds.map { case line =>

      val data       = JSON.parseObject(line)
      val vin        = data.getString("vin")
      val createTime = data.getLong("createTime")
      val mileage    = data.getLong("mileage")

      // 将每行数据转换为 SourceData 数据源
      SourceData(vin, createTime, mileage)
    }

    val tripIdleTimeout: Duration = Duration(3 * 60 * 1000)  // 状态闲置时长，超时时间 3 分钟
    val tripDuration: Duration    = Duration(1 * 60 * 1000)  // 两个行程间隔时长，1 分钟

    val finish = events
      .groupByKey(event => event.vin)
      .mapGroupsWithState[TripInfo, TripUpdate](GroupStateTimeout.ProcessingTimeTimeout) {

      case (vin: String, source: Iterator[SourceData], state: GroupState[TripInfo]) =>
        if (state.hasTimedOut && state.exists) { // 超时
          TripUpdate(
            vin,
            state.get.tripStartTime,
            state.get.tripEndTime,
            state.get.startMileage,
            state.get.endMileage,
            state.get.tripDuration,
            state.get.tripDistance,
            tripStatus = 0 // 超时结束的行程
          )
        } else {
          // Iterator 消费一次就没有了, 下面多次用到 source 源数据, 所以要转成 Seq,
          // 否则第二个使用 source 的函数接受到的 data 就是空的了, 会报错
          val data = source.toSeq
          var lastTrip = getLastState(data, state) // 获取该车旧的行程状态

          // 更新行程信息
          if (state.exists) {
            state.setTimeoutDuration(tripIdleTimeout.milliseconds)
            val lastTripInfo = state.get // 获取旧的行程的状态

            // 找到第一个 1 分钟未上传数据的点
            data.map{ _.createTime * 1000L - lastTrip.tripEndTime * 1000L - tripDuration.milliseconds }.foreach(println(_))
            val guard: Option[SourceData] = data.find(_.createTime * 1000L - lastTrip.tripEndTime * 1000L - tripDuration.milliseconds > 0)

            guard match {
              case Some(d) => { // 划分行程
                println("开始划分新的行程了： ", state.get.tripStartTime, state.get.startMileage, state.get.tripEndTime, state.get.endMileage)
                initState(data, state) // 初始化一个新的行程
                println("初始化新的行程后： ", state.get.tripStartTime, state.get.startMileage, state.get.tripEndTime, state.get.endMileage)

                // bug 把上一个行程刷到 console
                println("lastTripInfo: ", lastTripInfo.tripStartTime, lastTripInfo.tripEndTime)

                TripUpdate(
                  vin,
                  tripStartTime = lastTripInfo.tripStartTime,
                  tripEndTime   = lastTripInfo.tripEndTime,
                  startMileage  = lastTripInfo.startMileage,
                  endMileage    = lastTripInfo.endMileage,
                  tripDuration  = lastTripInfo.tripDuration,
                  tripDistance  = lastTripInfo.tripDistance,
                  tripStatus    = 1 // 正常结束的行程
                )

              }

              case _ => {
                val updatedTripInfo = getUpdateState(data, state)
                state.update(updatedTripInfo)
                TripUpdate(
                  vin,
                  tripStartTime = state.get.tripStartTime,
                  tripEndTime   = state.get.tripEndTime,
                  startMileage  = state.get.startMileage,
                  endMileage    = state.get.endMileage,
                  tripDuration  = state.get.tripDuration,
                  tripDistance  = state.get.tripDistance,
                  tripStatus    = 2 // 正常进行中的行程
                )
              }
            }

          } else { // state 不存在, 则为第一次新进来的数据, 那么初始化一个初始状态
            initState(data, state)
            println("initState: ", state.get.tripStartTime, state.get.startMileage, state.get.tripEndTime, state.get.endMileage)
            println("current State: ", state.get.tripStartTime, state.get.startMileage, state.get.tripEndTime, state.get.endMileage)
            val updatedTripInfo = TripInfo(
              state.get.tripStartTime,
              state.get.tripEndTime,
              state.get.startMileage,
              state.get.endMileage
            )

            state.update(updatedTripInfo)

            TripUpdate(
              vin,
              tripStartTime = state.get.tripStartTime,
              tripEndTime   = state.get.tripEndTime,
              startMileage  = state.get.startMileage,
              endMileage    = state.get.endMileage,
              tripDuration  = state.get.tripDuration,
              tripDistance  = state.get.tripDistance,
              tripStatus    = 0 // 初始的行程
            )
          }
        }
    }

    finish.writeStream
      .outputMode(OutputMode.Update())
      .trigger(Trigger.ProcessingTime("1 seconds"))
      .format("console")
      .option("truncate", "false") // 不截断显示
      .start()
      .awaitTermination()
  }

  /**
    * @param sourceData 新的源数据序列
    * @param state 旧的状态
    * @return
    */
  def getUpdateState(sourceData: Seq[SourceData], state: GroupState[TripInfo]): TripInfo = {
    var tripStartTime: Long = 0
    var tripEndTime: Long = 0
    var startMileage: Long = 0
    var endMileage: Long = 0

    tripStartTime = state.get.tripStartTime
    startMileage  = state.get.startMileage
    tripEndTime   = sourceData.map(_.createTime).max // 更新 tripEndTime
    endMileage    = sourceData.map(_.mileage).max    // 更新 endMileage

    TripInfo(
      tripStartTime,
      tripEndTime,
      startMileage,
      endMileage
    )
  }

  /**
    * 获取上一个行程的状态
    * @param sourceData 新的源数据序列
    * @param state 内存中的旧的状态
    * @return 刷新后的行程信息
    */
  def getLastState(sourceData: Seq[SourceData], state: GroupState[TripInfo]): TripInfo = {
    if (state.exists) {
      state.get
    } else { // 如果状态不存在, 则 sourceData 是新的源数据

      val tripStartTime = sourceData.map(_.createTime).min
      val startMileage  = sourceData.map(_.mileage).min

      TripInfo(
        tripStartTime,
        tripEndTime = tripStartTime, // 将新的源数据序列中的 createTime 最小值作为行程开始时间
        startMileage,
        endMileage = startMileage    // 将新的源数据序列中的 mileage 最小值作为行程开始里程
      )
    }
  }

  /**
    * 初始化下一个行程
    * @param sourceData 新的源数据序列
    * @param state 内存中的旧的 state
    */
  def initState(sourceData: Seq[SourceData], state: GroupState[TripInfo]): Unit = {

    val tripStartTime = sourceData.map(_.createTime).min
    val startMileage  = sourceData.map(_.mileage).min

    val initTripInfo = TripInfo(
      tripStartTime,
      tripEndTime = tripStartTime,
      startMileage,
      endMileage = startMileage
    )
    state.update(initTripInfo)
  }
}