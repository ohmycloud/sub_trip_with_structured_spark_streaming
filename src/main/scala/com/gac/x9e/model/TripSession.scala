package com.gac.x9e.model

/**
  * 更新后的行程
  * @param vin           车架号
  * @param tripStartTime 行程开始时间
  * @param tripEndTime   行程结束时间
  * @param startMileage  开始里程数
  * @param endMileage    结束里程数
  * @param tripDuration  行驶时长
  * @param tripDistance  行驶距离
  * @param isTripEnded   行程状态 true-结束, false-进行中
  */
case class TripSession(
  vin:               String,
  var tripStartTime: Long,
  var tripEndTime:   Long,
  var startMileage:  Long,
  var endMileage:    Long,
  var tripDuration:  Long,
  var tripDistance:  Long,
  var isTripEnded:   Boolean
)

object TripSession {

}
