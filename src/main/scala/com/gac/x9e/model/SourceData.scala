package com.gac.x9e.model

import java.sql.Timestamp

/**
  * 数据源
 *
  * @param vin        车架号
  * @param createTime 信号发生时间
  * @param mileage    当前里程
  */
case class SourceData (
  vin:        String,
  createTime: Timestamp,
  mileage:    Long
)

object SourceData {}