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

  override def extract(spark: SparkSession, ds: Dataset[SourceData]): Dataset[TripSession] = {
      import spark.implicits._
      ds.withWatermark("createTime", waterMarkDuration)
        .groupByKey(_.vin)
        .flatMapGroupsWithState(
          outputMode  = OutputMode.Update(),
          timeoutConf = GroupStateTimeout.EventTimeTimeout()
        )(func = mappingFunction)
  }

  private def mappingFunction(vin: String,
                              source: Iterator[SourceData],
                              state: GroupState[TripState]): Iterator[TripSession] = {
    val sourceData = source.toArray.sortBy(_.createTime.getTime) // 按时间升序
    // 声明一个数组用于存放划分后的可能的多个行程
    val tripResult: ArrayBuffer[TripSession] = ArrayBuffer[TripSession]()

    if (state.hasTimedOut) {
      val currentState: Option[TripState] = state.getOption
      println(s"$vin timeout with state: {$currentState}")

      state.remove() // 超时则移除

      for {
        trip <- currentState
        timeoutTrip = endedTripSession(vin, trip)
      } tripResult.append(timeoutTrip)
    } else {
      updateTripState(vin = vin, source = sourceData, state = state, tripResult = tripResult)
    }

    tripResult.iterator
  }

  private def initTripState(s: SourceData): TripState = {
    TripState(
      tripStartTime = s.createTime.getTime,
      tripEndTime = s.createTime.getTime,
      startMileage = s.mileage,
      endMileage = s.mileage
    )
  }

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

  private def updateTripState(vin: String,
                              source: Array[SourceData],
                              state: GroupState[TripState],
                              tripResult: ArrayBuffer[TripSession]): Unit = {
    val currentState = state.getOption
    if (currentState.isEmpty) {
      state.update(initTripState(source.head))   // 用第一条数据初始化一个状态
      state.setTimeoutTimestamp(timeoutDuration) // 设置超时时间
    }

    for (s <- source) {
      // 超过规定的 GapDuration, 结束上一个行程, 并开启下一个行程
      if (s.createTime.getTime - state.get.tripEndTime > tripGapDuration) {
        val endTrip = endedTripSession(vin, state.get)
        tripResult.append(endTrip)
        state.update(initTripState(s)) // 初始化下一个行程
      } else {
        // 行程进行中, 更新 state
        val updateTripState = TripState(
          tripStartTime = state.get.tripStartTime, // 行程开始时间保持不变
          tripEndTime   = s.createTime.getTime,    // 更新行程结束时间
          startMileage  = state.get.startMileage,  // 行程开始里程保持不变
          endMileage    = s.mileage                // 更新行程结束里程
        )
        state.update(updateTripState)
      }
    }
  }
}
