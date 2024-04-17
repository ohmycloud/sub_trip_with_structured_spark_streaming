package com.gac.x9e.core.impl

import com.gac.x9e.core.SubTrip
import com.gac.x9e.model.{SourceData, TripState, TripSession}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.collection.mutable.ArrayBuffer

object SubTripImpl extends SubTrip {
  private val tripGapDuration = 30 * 1000L    // 形成之间的 Gap 时间为 30秒
  private val timeoutDuration = 30 * 1000L    // 超时时间
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
    val currentState: Option[TripState] = state.getOption

    if (state.hasTimedOut) {
      println(s"$vin timeout with state: {$currentState}")
      state.remove() // 超时则移除

      for {
        trip <- state.getOption
      } yield TripSession(
        vin = vin,
        tripStartTime = trip.tripStartTime,
        tripEndTime = trip.tripEndTime,
        startMileage = trip.startMileage,
        endMileage = trip.endMileage,
        tripDuration = trip.tripDuration,
        tripDistance = trip.tripDistance,
        isTripEnded = true
      )
    } else if (state.exists) { // 状态存在
      updateTripState(vin = vin, source = sourceData, state = state, tripResult = tripResult)
    } else {
      val headData = sourceData.head // 第一条数据
      // 用第一条数据初始化一个状态
      val initTripState = TripState(
        tripStartTime = headData.createTime.getTime,
        tripEndTime   = headData.createTime.getTime,
        startMileage  = headData.mileage,
        endMileage    = headData.mileage
      )
      state.update(initTripState)
      updateTripState(vin = vin, source = sourceData, state = state, tripResult = tripResult)
      state.setTimeoutTimestamp(timeoutDuration) // Set the timeout
    }

    tripResult.iterator
  }

  private def updateTripState(vin: String,
                              source: Array[SourceData],
                              state: GroupState[TripState],
                              tripResult: ArrayBuffer[TripSession]): Unit = {
    for (s <- source) {
      // 超过 GapDuration 就划分一次会话
      if (s.createTime.getTime - state.get.tripEndTime > tripGapDuration) {
        val endTrip = TripSession(
          vin = vin,
          tripStartTime = state.get.tripStartTime,
          tripEndTime   = state.get.tripEndTime,
          startMileage  = state.get.startMileage,
          endMileage    = state.get.endMileage,
          tripDuration  = state.get.tripDuration,
          tripDistance  = state.get.tripDistance,
          isTripEnded   = true
        )

        tripResult.append(endTrip)

        // 初始化下一个行程
        val initTripState = TripState(
          tripStartTime = s.createTime.getTime,
          tripEndTime   = s.createTime.getTime,
          startMileage  = s.mileage,
          endMileage    = s.mileage
        )
        state.update(initTripState)
      } else {
        // 行程进行中, 更新
        val updateTripState = TripState(
          tripStartTime = state.get.tripStartTime,
          tripEndTime   = s.createTime.getTime,
          startMileage  = state.get.startMileage,
          endMileage    = s.mileage
        )
        state.update(updateTripState)
      }
    }
  }
}
