package com.gac.x9e.model

import java.sql.Timestamp


/**
 * 储能状态数据
 * @param stationId 场站编号
 * @param dataTime 数据时间
 */
case class IotData(
                    stationId: String,
                    dataTime: Timestamp
                  )

/**
 * 储能场站状态
 * @param stationId 场站编号
 * @param startTime 数据开始时间
 * @param endTime 数据结束时间
 * @param status 储能设备状态, true-数据传输结束 false-数据传输中
 */
case class IotEvent(
                      stationId: String,
                      startTime: Long,
                      endTime: Long,
                      status: Boolean
                    )