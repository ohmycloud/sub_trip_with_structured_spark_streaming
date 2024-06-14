package com.gac.x9e.conf

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Kafka 配置
  */
class KafkaConfiguration extends Serializable{
  private val config:                   Config  = ConfigFactory.load()
  lazy    val kafkaConfig:              Config  = config.getConfig("kafka")
  lazy    val bootstrapServers:         String  = kafkaConfig.getString("common.bootstrap.servers")
  lazy    val topic:                    String  = kafkaConfig.getString("common.topic")
  lazy val startingOffsets:             String  = kafkaConfig.getString("common.source.offsets")
  lazy val maxOffsetsPerTrigger:        String  = kafkaConfig.getString("common.maxOffsetsPerTrigger")
  lazy val failOnDataLoss:              Boolean = kafkaConfig.getBoolean("common.failOnDataLoss")
  lazy val maxRequestSize:              Int     = kafkaConfig.getInt("common.max.request.size")
}