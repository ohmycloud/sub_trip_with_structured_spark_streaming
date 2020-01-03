package com.gac.x9e.module

import com.gac.x9e.SubTripApplication
import com.gac.x9e.conf.{KafkaConfiguration, SocketConfiguration, SparkConfiguration}
import com.gac.x9e.core.{NaAdapter, NaSubTrip}
import com.gac.x9e.core.impl.{NaAdapterImpl, NaSubTripImpl}
import com.gac.x9e.pipeline.{NaSource, NaSparkSession}
import com.google.inject.{AbstractModule, Provides, Singleton}
import org.apache.spark.sql.{DataFrame, SparkSession}

object MainModule extends AbstractModule {
  override def configure(): Unit = {
    bind(classOf[SparkConfiguration]).asEagerSingleton() // Spark 配置
    bind(classOf[KafkaConfiguration]).asEagerSingleton() // Kafka 配置

    bind(classOf[SubTripApplication])                    // 程序入口
    bind(classOf[NaAdapter]).toInstance(NaAdapterImpl)   // 国标数据适配
    bind(classOf[NaSubTrip]).toInstance(NaSubTripImpl)   // 国标行程划分
  }

  /**
   * 获取国标 SparkSession
   * @param sparkConf Spark 配置
   * @return SparkSession
   */
  @Provides
  @Singleton
  def NaSparkSession(sparkConf: SparkConfiguration): NaSparkSession[SparkSession] = {
    new NaSparkSession[SparkSession] {
      override def session(): SparkSession = {
        val sparkConf =  new SparkConfiguration
        SparkSession.builder
          .master(sparkConf.sparkMaster)
          .appName("subtrip using stateful structured streaming")
          .getOrCreate()
      }
    }
  }

  /**
   *
   * @param socketConf Socket 配置
   * @param sparkConf Spark 配置
   * @return
   */
  @Provides
  @Singleton
  def NaDataSource(socketConf: SocketConfiguration, sparkConf: SparkConfiguration): NaSource[DataFrame] = {
    new NaSource[DataFrame] {
      override def stream(): DataFrame = {

        val socketConf = new SocketConfiguration
        val sparkConf  = new SparkConfiguration
        val spark = NaSparkSession(sparkConf).session()

        spark
          .readStream
          .format("socket")
          .option("host", socketConf.host)
          .option("port", socketConf.port)
          .load()
      }
    }
  }
}

