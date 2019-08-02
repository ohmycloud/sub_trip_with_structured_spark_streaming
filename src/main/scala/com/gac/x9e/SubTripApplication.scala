package com.gac.x9e

import com.gac.x9e.SubTripApplication.Params
import com.gac.x9e.conf.{KafkaConfiguration, SparkConfiguration}
import com.gac.x9e.core.{EnAdapter, NaAdapter, NaSubTrip}
import com.gac.x9e.module.MainModule
import com.gac.x9e.pipeline.{EnSource, EnSparkSession, NaSource, NaSparkSession}
import com.google.inject.{Guice, Inject, Singleton}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scopt.OptionParser

object SubTripApplication extends App {
  val parser = new OptionParser[Params]("SubTripApplication") {
    head("SubTripApplication")

    opt[String]('c', "conf")
      .text("config.resource for x9e-gac")
      .action((x, c) => c.copy(conf = x))

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

  case class Params(conf: String = "", interval: Int = 360)
}

@Singleton
class SubTripApplication @Inject() (
                                     naSparkSession:  NaSparkSession[SparkSession],
                                     naSource:        NaSource[DataFrame],
                                     naAdapter:       NaAdapter,
                                     enSparkSession:  EnSparkSession[SparkSession],
                                     naSubTrip:       NaSubTrip,
                                     enSource:        EnSource[DataFrame],
                                     enAdapter:       EnAdapter
                                   ) extends Serializable {
  private def createNewStreamingQuery(params: Params): Unit = {
    val sparkConf = new SparkConfiguration
    val kafkaConf = new KafkaConfiguration
    val spark = naSparkSession.session()

    val naAdapterDf = naAdapter.extract(spark, naSource.stream())
    val naSubTripDs = naSubTrip.extract(spark, naAdapterDf)
    val trips = naSubTripDs
     // .withWatermark("createTime", "60 seconds")
      .writeStream
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .outputMode(OutputMode.Update())
      .start

    spark.streams.awaitAnyTermination()

  }

  def run(params: Params): Unit = {
    createNewStreamingQuery(params)
  }
}