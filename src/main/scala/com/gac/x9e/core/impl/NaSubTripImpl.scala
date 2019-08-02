package com.gac.x9e.core.impl

import com.gac.x9e.core.NaSubTrip
import com.gac.x9e.model.{SourceData, TripInfo, TripUpdate}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.collection.mutable.ArrayBuffer

object NaSubTripImpl extends NaSubTrip {
  override def extract(spark: SparkSession, ds: Dataset[SourceData]): Dataset[TripUpdate] = {
    synchronized {
      import spark.implicits._
      ds
        .groupByKey(event => event.vin)
        // .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout)(mappingFunction)
        .flatMapGroupsWithState(
          outputMode  = OutputMode.Update(),
          timeoutConf = GroupStateTimeout.ProcessingTimeTimeout()
        )(func = mappingFunction)
    }

  }

  def mappingFunction(vin: String, source: Iterator[SourceData], state: GroupState[TripInfo]): Iterator[TripUpdate] = {

    // 声明一个数组用于存放划分后的可能的多个行程
    val tripResult: ArrayBuffer[TripUpdate] = ArrayBuffer[TripUpdate]()
    tripResult.clear()

    if (state.hasTimedOut) {
      state.remove() // 超时则移除
    } else if (state.exists) { // 状态存在
      val sourceData = source.toArray.sortBy(_.createTime) // 按时间升序
      for (s <- sourceData) {
        if (s.createTime - state.get.tripEndTime > 5) {
          val endTrip = TripUpdate(
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
          val initTripInfo = TripInfo(
            tripStartTime = s.createTime,
            tripEndTime   = s.createTime,
            startMileage  = s.mileage,
            endMileage    = s.mileage
          )
          state.update(initTripInfo)
        } else { // update
          val updateTripInfo = TripInfo(
            tripStartTime = state.get.tripStartTime,
            tripEndTime   = s.createTime,
            startMileage  = state.get.startMileage,
            endMileage    = s.mileage
          )
          state.update(updateTripInfo)
        }
      }
    } else {
      val sourceData = source.toArray.sortBy(_.createTime) // 按时间升序
      val headData = sourceData.head // 第一条数据
      // 用第一条数据初始化一个状态
      val initTripInfo = TripInfo(
        tripStartTime = headData.createTime,
        tripEndTime   = headData.createTime,
        startMileage  = headData.mileage,
        endMileage    = headData.mileage
      )
      state.update(initTripInfo)


      for (s <- sourceData.tail) {
        if (s.createTime - state.get.tripEndTime > 5) { // end
          val endTrip = TripUpdate(
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
          val initTripInfo = TripInfo(
            tripStartTime = s.createTime,
            tripEndTime   = s.createTime,
            startMileage  = s.mileage,
            endMileage    = s.mileage
          )
          state.update(initTripInfo)

        } else { // update
          val updateTripInfo = TripInfo(
            tripStartTime = state.get.tripStartTime,
            tripEndTime   = s.createTime,
            startMileage  = state.get.startMileage,
            endMileage    = s.mileage
          )
          state.update(updateTripInfo)
        }
      }

      state.setTimeoutDuration("30 seconds") // Set the timeout
    }

    tripResult.iterator
  }
}
