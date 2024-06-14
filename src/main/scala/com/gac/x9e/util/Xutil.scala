package com.gac.x9e.util

import com.gac.x9e.conf.{KafkaConfiguration, SparkConfiguration}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Xutil {
  /** 创建 SparkSession
   *
   * @return
   */
  def iotSparkSession(sparkConf: SparkConfiguration): SparkSession = {
    SparkSession.builder
      .master(sparkConf.sparkMaster)
      .config(
        "spark.sql.streaming.checkpointLocation",
        sparkConf.checkpoint
      )
      .appName(sparkConf.appName)
      .getOrCreate()
  }

  /** 消费 Kafka 消息
   *
   * @return
   */
  def KafkaDataSource(
                       spark: SparkSession,
                       kafkaConf: KafkaConfiguration
                     ): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaConf.bootstrapServers)
      .option("subscribe", kafkaConf.topic)
      .option("startingOffsets", kafkaConf.startingOffsets)
      .option("maxOffsetsPerTrigger", kafkaConf.maxOffsetsPerTrigger)
      .option("failOnDataLoss", value = kafkaConf.failOnDataLoss)
      .option("kafka.max.request.size", kafkaConf.maxRequestSize)
      .load()
      .select("value")
  }
}
