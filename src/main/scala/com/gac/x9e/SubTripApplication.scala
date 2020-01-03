package com.gac.x9e

import com.gac.x9e.SubTripApplication.Params
import com.gac.x9e.core.{NaAdapter, NaSubTrip}
import com.gac.x9e.module.MainModule
import com.gac.x9e.pipeline.{NaSource, NaSparkSession}
import com.google.inject.{Guice, Inject, Singleton}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}
import scopt.OptionParser

object SubTripApplication extends App {
  val parser = new OptionParser[Params]("SubTripApplication") {
    head("SubTripApplication")

    opt[Int]('i', "interval")
      .text("config.resource for x9e-gac")
      .action((x, c) => c.copy(interval = x))

    help("help").text("prints this usage text")
  }

  parser.parse(args, Params()) match {
    case Some(params) =>
      val injector = Guice.createInjector(MainModule)
      val runner = injector.getInstance(classOf[SubTripApplication])
      ConfigFactory.invalidateCaches()
      runner.run(params)
    case _ => sys.exit(1)
  }

  case class Params(interval: Int = 360)
}

@Singleton
class SubTripApplication @Inject() (
                                     naSparkSession:  NaSparkSession[SparkSession],
                                     naSource:        NaSource[DataFrame],
                                     naAdapter:       NaAdapter,
                                     naSubTrip:       NaSubTrip
                                   ) extends Serializable {
  private def createNewStreamingQuery(params: Params): Unit = {
    val spark = naSparkSession.session()
    spark.sparkContext.setLogLevel("WARN")

    val naAdapterDf = naAdapter.extract(spark, naSource.stream())
    val naSubTripDs = naSubTrip.extract(spark, naAdapterDf)
    val trips = naSubTripDs
      .writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode(OutputMode.Update())
      .start

    spark.streams.awaitAnyTermination()

  }

  def run(params: Params): Unit = {
    createNewStreamingQuery(params)
  }
}