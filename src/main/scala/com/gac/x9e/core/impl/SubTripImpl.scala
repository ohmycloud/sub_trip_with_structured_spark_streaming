package com.gac.x9e.core.impl

import com.gac.x9e.core.SubTrip
import com.gac.x9e.model.{SourceData, TripState, TripSession}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.collection.mutable.ArrayBuffer

object SubTripImpl extends SubTrip {
  private val tripGapDuration = 30 * 1000L    // 形成之间的 Gap 时间为 30秒
  private val timeoutDuration = 10 * 1000L    // 超时时间
  private val waterMarkDuration = "5 minutes" // 水位

  /**
   * 行程划分
   * @param spark SparkSession
   * @param ds SourceData 数据集
   * @return TripSession 数据集
   */
  override def extract(spark: SparkSession, ds: Dataset[SourceData]): Dataset[TripSession] = {
      import spark.implicits._
      ds.withWatermark("createTime", waterMarkDuration)
        .groupByKey(_.vin)
        .flatMapGroupsWithState(
          outputMode  = OutputMode.Update(),
          timeoutConf = GroupStateTimeout.EventTimeTimeout()
        )(func = mappingFunction)
  }

  /**
   *
   * @param vin 车架号
   * @param source 数据源
   * @param state TripState 状态
   * @return 一组 TripSession
   */
  private def mappingFunction(vin: String,
                              source: Iterator[SourceData],
                              state: GroupState[TripState]): Iterator[TripSession] = {
    val sourceData = source.toArray.sortBy(_.createTime.getTime) // 按时间升序
    // 声明一个数组用于存放划分后的可能的多个行程
    val tripResult: ArrayBuffer[TripSession] = ArrayBuffer[TripSession]()

    if (state.hasTimedOut) {
      // optionally copy a `TripState`,
      // In the following for-comprehension, currentState maybe a Some(TripState),
      // after the state been removed.
      val currentState: Option[TripState] = state.getOption
      println(s"$vin timeout with state: {$currentState}")

      state.remove() // 超时则移除

      for {
        trip <- currentState
        timeoutTrip = endedTripSession(vin, trip)
      } tripResult.append(timeoutTrip)
    } else {
      updateState(vin = vin, source = sourceData, state = state, tripResult = tripResult)
    }

    tripResult.iterator
  }

  /**
   * 用车辆行程数据初始化行程状态
   * @param s 数据源
   * @return TripState
   */
  private def initTripState(s: SourceData): TripState = {
    TripState(
      tripStartTime = s.createTime.getTime,
      tripEndTime   = s.createTime.getTime,
      startMileage  = s.mileage,
      endMileage    = s.mileage
    )
  }

  /**
   * 生成行程结束会话
   * @param vin 车架号
   * @param t TripState
   * @return TripSession
   */
  private def endedTripSession(vin: String, t: TripState): TripSession = {
    TripSession(
      vin = vin,
      tripStartTime = t.tripStartTime,
      tripEndTime   = t.tripEndTime,
      startMileage  = t.startMileage,
      endMileage    = t.endMileage,
      tripDuration  = t.tripDuration,
      tripDistance  = t.tripDistance,
      isTripEnded   = true
    )
  }

  /**
   * 生成新的 TripState, 用于更新
   * @param s 数据源
   * @param t TripState
   * @return 更新后的 TripState
   */
  private def updateTripState(s: SourceData, t: TripState): TripState = {
    TripState(
      tripStartTime = t.tripStartTime,      // 行程开始时间保持不变
      tripEndTime   = s.createTime.getTime, // 更新行程结束时间
      startMileage  = t.startMileage,       // 行程开始里程保持不变
      endMileage    = s.mileage             // 更新行程结束里程
    )
  }

  /**
   * 如果状态没有超时, 则更新状态
   * @param vin 车架号
   * @param source 数据源
   * @param state TripState 车辆行程状态
   * @param tripResult 车辆行程结果
   */
  private def updateState(vin: String,
                              source: Array[SourceData],
                              state: GroupState[TripState],
                              tripResult: ArrayBuffer[TripSession]): Unit = {
    // 如果状态不存在
    if (state.getOption.isEmpty) {
      println(s"Is state for $vin exists: ", state.exists)
      state.update(initTripState(source.head))   // 用第一条数据初始化一个状态
      state.setTimeoutTimestamp(state.getCurrentWatermarkMs() + timeoutDuration) // 设置超时时间
    }

    for {
      s <- source
      tripState <- state.getOption
    } {
      // 超过规定的 GapDuration, 结束上一个行程, 并开启下一个行程
      if (s.createTime.getTime - tripState.tripEndTime > tripGapDuration) {
        val endTrip = endedTripSession(vin, tripState)
        tripResult.append(endTrip)
        state.update(initTripState(s)) // 初始化下一个行程
      } else {
        // 行程进行中, 更新 state
        val updatedTripState = updateTripState(s, tripState)
        state.update(updatedTripState)
      }
      state.setTimeoutTimestamp(state.getCurrentWatermarkMs() + timeoutDuration) // 设置超时时间
    }
  }
}
