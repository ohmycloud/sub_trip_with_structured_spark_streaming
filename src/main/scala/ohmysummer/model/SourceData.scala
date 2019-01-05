package ohmysummer.model

/**
  * 数据源
  * @param vin        车架号
  * @param createTime 信号发生时间
  * @param mileage    当前里程
  */
case class SourceData (
  vin:        String,
  createTime: Long,
  mileage:    Long
)

object SourceData {}