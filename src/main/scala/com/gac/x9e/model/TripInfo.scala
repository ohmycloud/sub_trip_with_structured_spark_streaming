package com.gac.x9e.model

/**
  * 行程信息
  * @param tripStartTime 行程开始时间
  * @param tripEndTime   行程结束时间
  * @param startMileage  开始里程数
  * @param endMileage    结束里程数
  */
case class TripInfo(
  var tripStartTime: Long,
  var tripEndTime:   Long,
  var startMileage:  Long,
  var endMileage:    Long
) {
    def tripDuration: Long = tripEndTime - tripStartTime
    def tripDistance: Long = endMileage  - startMileage
  }

object TripInfo {

}
