package com.gac.x9e.core.impl

import com.gac.x9e.core.NaSubTrip
import com.gac.x9e.model.{SourceData, TripState, TripSession}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.collection.mutable.ArrayBuffer

object NaSubTripImpl extends NaSubTrip {
  override def extract(spark: SparkSession, ds: Dataset[SourceData]): Dataset[TripSession] = {
      import spark.implicits._
      ds.withWatermark("createTime", "30 seconds") // 设置水位
        .groupByKey(event => event.vin)
        .flatMapGroupsWithState(
          outputMode  = OutputMode.Update(),
          timeoutConf = GroupStateTimeout.EventTimeTimeout()
        )(func = mappingFunction)
  }

  def mappingFunction(vin: String, source: Iterator[SourceData], state: GroupState[TripState]): Iterator[TripSession] = {

    // 声明一个数组用于存放划分后的可能的多个行程
    val tripResult: ArrayBuffer[TripSession] = ArrayBuffer[TripSession]()

    tripResult.clear()
    val sourceData = source.toArray.sortBy(_.createTime.getTime) // 按时间升序

    if (state.hasTimedOut) {
      state.remove() // 超时则移除
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
      state.setTimeoutTimestamp(30000) // Set the timeout
    }

    tripResult.iterator
  }

  def updateTripState(vin: String, source: Array[SourceData], state: GroupState[TripState], tripResult: ArrayBuffer[TripSession]): Unit = {
    for (s <- source) {
      if (s.createTime.getTime - state.get.tripEndTime > 5000) { // 超过 5 秒就划分一次会话
        val endTrip = TripSession(
          vin = vin,
          tripStartTime = state.get.tripStartTime,
          tripEndTime   = state.get.tripEndTime,
          startMileage  = state.get.startMileage,
          endMileage    = state.get.endMileage,
          tripDuration  = state.get.tripDuration,
          tripDistance  = state.get.tripDistance,
          tripStatus    = 0
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
      } else { // update
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
