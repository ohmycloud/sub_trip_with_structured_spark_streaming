package com.gac.x9e

import java.sql.Timestamp
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}

object XS6SubTrip {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("Structured Sessionization Redux")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    implicit val ctx = spark.sqlContext
    val input = MemoryStream[String]

    val EVENT_SCHEMA = new StructType()
      .add($"event_time".string)
      .add($"user_id".string)

    val events = input.toDS()
      .select(from_json($"value", EVENT_SCHEMA).alias("json"))
      .select($"json.*")
      .withColumn("event_time", to_timestamp($"event_time"))
      .withWatermark("event_time", "1 hours")
    events.printSchema()

    val sessionized = events
      .groupByKey(row => row.getAs[String]("user_id"))
      .mapGroupsWithState[SessionState, SessionOutput](GroupStateTimeout.EventTimeTimeout) {
        case (userId: String, events: Iterator[Row], state: GroupState[SessionState]) =>
          println(s"state update for user ${userId} (current watermark: ${new Timestamp(state.getCurrentWatermarkMs())})")
          if (state.hasTimedOut) {
            println(s"User ${userId} has timed out, sending final output.")
            val finalOutput = SessionOutput(
              userId = userId,
              startTimestampMs = state.get.startTimestampMs,
              endTimestampMs = state.get.endTimestampMs,
              durationMs = state.get.durationMs,
              expired = true
            )
            // Drop this user's state
            state.remove()
            finalOutput
          } else {
            // 获取 Timestamp 列表
            val timestamps = events.map(_.getAs[Timestamp]("event_time").getTime).toSeq
            println(s"User ${userId} has new events (min: ${new Timestamp(timestamps.min)}, max: ${new Timestamp(timestamps.max)}).")
            val newState = if (state.exists) {
              println(s"User ${userId} has existing state.")
              val oldState = state.get
              SessionState(
                startTimestampMs = math.min(oldState.startTimestampMs, timestamps.min),
                endTimestampMs = math.max(oldState.endTimestampMs, timestamps.max)
              )
            } else {
              println(s"User ${userId} has no existing state.")
              SessionState(
                startTimestampMs = timestamps.min,
                endTimestampMs = timestamps.max
              )
            }
            state.update(newState)
            state.setTimeoutTimestamp(newState.endTimestampMs, "30 minutes")
            println(s"User ${userId} state updated. Timeout now set to ${new Timestamp(newState.endTimestampMs + (30 * 60 * 1000))}")
            SessionOutput(
              userId = userId,
              startTimestampMs = state.get.startTimestampMs,
              endTimestampMs = state.get.endTimestampMs,
              durationMs = state.get.durationMs,
              expired = false
            )
          }
      }

    val eventsQuery = sessionized
      .writeStream
      .queryName("events")
      .outputMode("update")
      .format("console")
      .start()

    input.addData(
      """{"event_time": "2018-01-01T00:00:00", "user_id": "mike"}""",
      """{"event_time": "2018-01-01T00:01:00", "user_id": "mike"}""",
      """{"event_time": "2018-01-01T00:05:00", "user_id": "mike"}"""
    )
    input.addData(
      """{"event_time": "2018-01-01T00:45:00", "user_id": "mike"}"""
    )
    eventsQuery.processAllAvailable()
  }

  case class SessionState(startTimestampMs: Long, endTimestampMs: Long) {
    def durationMs: Long = endTimestampMs - startTimestampMs
  }

  case class SessionOutput(userId: String, startTimestampMs: Long, endTimestampMs: Long, durationMs: Long, expired: Boolean)
}