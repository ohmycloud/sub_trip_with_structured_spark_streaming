package com.gac.x9e.module

import com.gac.x9e.SubTripApp
import com.gac.x9e.conf.{KafkaConfiguration, SocketConfiguration, SparkConfiguration}
import com.gac.x9e.core.{Adapter, SubTrip}
import com.gac.x9e.core.impl.{AdapterImpl, SubTripImpl}
import com.gac.x9e.pipeline.{DataSource, WrapperSparkSession}
import com.google.inject.{AbstractModule, Provides, Singleton}
import org.apache.spark.sql.{DataFrame, SparkSession}

object MainModule extends AbstractModule {
  override def configure(): Unit = {
    bind(classOf[SparkConfiguration]).asEagerSingleton() // Spark 配置
    bind(classOf[KafkaConfiguration]).asEagerSingleton() // Kafka 配置

    bind(classOf[SubTripApp])                            // 程序入口
    bind(classOf[Adapter]).toInstance(AdapterImpl)       // 数据适配
    bind(classOf[SubTrip]).toInstance(SubTripImpl)       // 行程划分
  }

  /**
   * 获取国标 SparkSession
   * @param sparkConf Spark 配置
   * @return SparkSession
   */
  @Provides
  @Singleton
  private def wrapperSparkSession(sparkConf: SparkConfiguration): WrapperSparkSession[SparkSession] = {
    new WrapperSparkSession[SparkSession] {
      override def session(): SparkSession = {
        val sparkConf =  new SparkConfiguration
        SparkSession.builder
          .master(sparkConf.sparkMaster)
          .appName("sub trip using stateful structured streaming")
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
  def dataSource(socketConf: SocketConfiguration, sparkConf: SparkConfiguration): DataSource[DataFrame] = {
    new DataSource[DataFrame] {
      override def stream(): DataFrame = {

        val socketConf = new SocketConfiguration
        val sparkConf  = new SparkConfiguration
        val spark = wrapperSparkSession(sparkConf).session()

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

