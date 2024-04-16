package com.gac.x9e

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

case class UserEvent(
                    id: Int,
                    greet: String,
                    isLast: Boolean
                    )

case class UserSession(userEvents: Seq[UserEvent])

object StatefulSessionApp extends App {
  private def updateSessionEvents(
                           id: Int,
                           userEvents: Iterator[UserEvent],
                           state: GroupState[UserSession]
                         ): Option[UserSession] = {
    if (state.hasTimedOut) {
      // We've timed out, lets extract the state and send it down the stream
      state.remove()
      println("Timeout with option state: ", state.getOption)
      state.getOption
    } else {
      // New data has come in for the given user id. We'll look up the current state to see if we already have
      // something stored. If not, we'll just take the current user events and update the state,
      // otherwise, we will concatenate the user events we already have with the new incoming events.
      val currentState: Option[UserSession] = state.getOption
      val updatedUserSession: UserSession = currentState.fold(UserSession(userEvents.toSeq))(currentUserSession =>
        UserSession(currentUserSession.userEvents ++ userEvents.toSeq)
      )
      state.update(updatedUserSession)

      if (updatedUserSession.userEvents.exists(_.isLast)) {
        // If we've received a flag indicating this should be the last event batch, let's close the state and
        // send the user session downstream.
        val userSession = state.getOption
        state.remove()
        userSession
      } else {
        state.setTimeoutDuration("30 seconds")
        None
      }
    }
  }

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Stateful Structured Streaming")
    .getOrCreate()

  import spark.implicits._

  implicit val userEventEncoder: Encoder[UserEvent] = Encoders.kryo[UserEvent]
  implicit val userSessionEncoder: Encoder[Option[UserSession]] = Encoders.kryo[Option[UserSession]]

  private val userEventStream: Dataset[UserEvent] = spark
    .readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", "3333")
    .load()
    .as[String]
    .map { line =>
      val data = JSON.parseObject(line)
      val id = data.getIntValue("id")
      val greet = data.getString("greet")
      val isLast = data.getBoolean("isLast")

      UserEvent(id, greet, isLast)
    }

  private val finishedUserSessionStream: Dataset[UserSession] =
    userEventStream
      .groupByKey(_.id)
      .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout())(updateSessionEvents)
      .flatMap(userSession => userSession)
  finishedUserSessionStream
    .writeStream
    .outputMode(OutputMode.Update())
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "c:\\temp\\stateful")
    .start()
    .awaitTermination()
}
