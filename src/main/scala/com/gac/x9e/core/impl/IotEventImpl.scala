package com.gac.x9e.core.impl

import com.gac.x9e.model.{IotData, IotEvent}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ArrayBuffer

object IotEventImpl {
  private val transferGapDuration = 30 * 1000L // 数据传输之间的 Gap 时间为 30秒
  private val timeoutDuration = 10 * 1000L     // 超时时间
  private val waterMarkDuration = "5 minutes"  // 水位

  /**
   * 数据传输段划分
   * @param spark SparkSession
   * @param ds SourceData 数据集
   * @return TransferSession 数据集
   */
  def extract(spark: SparkSession, ds: Dataset[IotData]): Dataset[IotEvent] = {
    import spark.implicits._
    ds.withWatermark("dataTime", waterMarkDuration)
      .groupByKey(_.stationId)
      .flatMapGroupsWithState(
        outputMode  = OutputMode.Update(),
        timeoutConf = GroupStateTimeout.EventTimeTimeout()
      )(func = mappingFunction)
  }

  /**
   *
   * @param stationId 场站编号
   * @param source 数据源
   * @param state TransferState 状态
   * @return 一组 TransferSession
   */
  private def mappingFunction(stationId: String,
                              source: Iterator[IotData],
                              state: GroupState[IotEvent]): Iterator[IotEvent] = {
    val sourceData = source.toArray.sortBy(_.dataTime.getTime) // 按时间升序
    // 声明一个数组用于存放划分后的可能的多个数据段
    val transferResult: ArrayBuffer[IotEvent] = ArrayBuffer[IotEvent]()

    if (state.hasTimedOut) {
      val currentState: Option[IotEvent] = state.getOption
      println(s"$stationId timeout with state: {$currentState}")

      val currentTime = state.getCurrentWatermarkMs()

      state.remove() // 超时则移除

      for {
        transfer <- currentState
        timeoutTransfer = endedTransferSession(stationId, transfer, isTransferEnded = true)
      } transferResult.append(timeoutTransfer.copy(startTime = currentTime))
    } else {
      updateState(stationId = stationId, source = sourceData, state = state, transferResult = transferResult)
    }

    transferResult.iterator
  }

  /**
   * 初始化行程状态
   * @param s 数据源
   * @return TransferState
   */
  private def initTransferState(s: IotData): IotEvent = {
    IotEvent(
      stationId = s.stationId,
      startTime   = s.dataTime.getTime,
      endTime  = s.dataTime.getTime,
      status    = false
    )
  }

  /**
   * 生成行程结束会话
   * @param stationId 场站编号
   * @param t TransferState
   * @return TransferSession
   */
  private def endedTransferSession(stationId: String, t: IotEvent, isTransferEnded: Boolean): IotEvent = {
    IotEvent(
      stationId = stationId,
      startTime = t.startTime,
      endTime   = t.endTime,
      status    = isTransferEnded
    )
  }

  /**
   * 生成新的 TransferState, 用于更新
   * @param s 数据源
   * @param t TransferState
   * @return 更新后的 TransferState
   */
  private def updateTransferState(s: IotData, t: IotEvent): IotEvent = {
    IotEvent(
      stationId = t.stationId,
      startTime = t.startTime,
      endTime   = s.dataTime.getTime,
      status    = false
    )
  }

  /**
   * 如果状态没有超时, 则更新状态
   * @param stationId 场站编号
   * @param source 数据源
   * @param state TransferState 车辆行程状态
   * @param transferResult 车辆行程结果
   */
  private def updateState(stationId: String,
                          source: Array[IotData],
                          state: GroupState[IotEvent],
                          transferResult: ArrayBuffer[IotEvent]): Unit = {
    // 如果状态不存在
    if (state.getOption.isEmpty) {
      println(s"Is state for $stationId exists: ", state.exists)
      state.update(initTransferState(source.head))   // 用第一条数据初始化一个状态
      state.setTimeoutTimestamp(state.getCurrentWatermarkMs() + timeoutDuration) // 设置超时时间
    }

    for {
      s <- source.zipWithIndex
      transferState <- state.getOption
    } {
      // 超过规定的 GapDuration, 结束上一个传输段, 并开启下一个传输段
      if (s._1.dataTime.getTime - transferState.endTime > transferGapDuration) {
        val endTransfer = endedTransferSession(stationId, transferState, isTransferEnded = true)
        transferResult.append(endTransfer.copy(startTime = s._1.dataTime.getTime))
        state.update(initTransferState(s._1)) // 初始化下一个传输段
      } else {
        // 数据传输进行中, 更新 state
        val updatedTransferState = updateTransferState(s._1, transferState)
        state.update(updatedTransferState)

        // 只在这个批次的最后一条数据到达时, 才输出行程状态
        for {
          currentState <- state.getOption
          if source.length == (s._2 + 1)
        } {
          transferResult.append(endedTransferSession(stationId, currentState, isTransferEnded = false))
        }
      }
      state.setTimeoutTimestamp(state.getCurrentWatermarkMs() + timeoutDuration) // 设置超时时间
    }
  }
}
