package com.gac.x9e

import com.gac.x9e.TripStatusApp.Params
import com.gac.x9e.core.{Adapter, TripStatus}
import com.gac.x9e.module.MainModule
import com.gac.x9e.pipeline.{DataSource, WrapperSparkSession}
import com.google.inject.{Guice, Inject, Singleton}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scopt.OptionParser

object TripStatusApp extends App {
  private val parser = new OptionParser[Params]("TripStatusApp") {
    head("TripStatusApp")

    opt[Int]('i', "interval")
      .text("config.resource for x9e-gac")
      .action((x, c) => c.copy(interval = x))

    help("help").text("prints this usage text")
  }

  parser.parse(args, Params()) match {
    case Some(params) =>
      val injector = Guice.createInjector(MainModule)
      val runner = injector.getInstance(classOf[TripStatusApp])
      ConfigFactory.invalidateCaches()
      runner.run(params)
    case _ => sys.exit(1)
  }

  case class Params(interval: Int = 360)
}

@Singleton
class TripStatusApp @Inject() (sparkSession: WrapperSparkSession[SparkSession],
                            dataSource:   DataSource[DataFrame],
                            adapter:      Adapter,
                            tripStatus:   TripStatus
                           ) extends Serializable {
  private def createNewStreamingQuery(params: Params): Unit = {
    val spark = sparkSession.session()
    spark.sparkContext.setLogLevel("WARN")

    val adapterDf = adapter.extract(spark, dataSource.stream())
    val tripStatusDs = tripStatus.extract(spark, adapterDf)

    tripStatusDs
      .writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode(OutputMode.Update())
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start

    spark.streams.awaitAnyTermination()

  }

  def run(params: Params): Unit = {
    createNewStreamingQuery(params)
  }
}