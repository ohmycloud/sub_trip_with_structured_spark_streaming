package com.gac.x9e.module

import com.gac.x9e.SubTripApplication
import com.gac.x9e.conf.{KafkaConfiguration, SocketConfiguration, SparkConfiguration}
import com.gac.x9e.core.{EnAdapter, NaAdapter, NaSubTrip}
import com.gac.x9e.core.impl.{EnAdapterImpl, NaAdapterImpl, NaSubTripImpl}
import com.gac.x9e.pipeline.{EnSource, EnSparkSession, NaSource, NaSparkSession}
import com.google.inject.{AbstractModule, Provides, Singleton}
import org.apache.spark.sql.{DataFrame, SparkSession}

object MainModule extends AbstractModule {
  override def configure(): Unit = {
    bind(classOf[SparkConfiguration]).asEagerSingleton() // Spark 配置
    bind(classOf[KafkaConfiguration]).asEagerSingleton() // Kafka 配置

    bind(classOf[SubTripApplication])                    // 程序入口
    bind(classOf[EnAdapter]).toInstance(EnAdapterImpl)   // 企标数据适配
    bind(classOf[NaAdapter]).toInstance(NaAdapterImpl)   // 国标数据适配
    bind(classOf[NaSubTrip]).toInstance(NaSubTripImpl)   // 国标行程划分

  }

  /**
   * 获取企标 SparkSession
   * @return SparkSession
   */
  @Provides
  @Singleton
  def sparkSession(): EnSparkSession[SparkSession] = {
    new EnSparkSession[SparkSession] {
      override def session(): SparkSession = {
        val sparkConf = new SparkConfiguration
        SparkSession.builder
          .master(sparkConf.sparkMaster)
          .appName("Stateful Structured Streaming for x9e Enterprise")
          .getOrCreate()
      }
    }
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
          .appName("Stateful Structured Streaming for x9e Nation")
          .config("spark.sql.session.timeZone", "UTC")
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
        val spark = sparkSession().session()

        val df: DataFrame = spark
          .readStream
          .format("socket")
          .option("host", socketConf.host)
          .option("port", socketConf.port)
          .load()
        df
      }
    }
  }

  @Provides
  @Singleton
  def EnDataSource(socketConf: SocketConfiguration, sparkConf: SparkConfiguration): EnSource[DataFrame] = {
    new EnSource[DataFrame] {
      override def stream(): DataFrame = {

        val socketConf = new SocketConfiguration
        val spark = sparkSession().session()

        val df: DataFrame = spark
          .readStream
          .format("socket")
          .option("host", socketConf.host)
          .option("port", socketConf.port)
          .load()
        df
      }
    }
  }
}

