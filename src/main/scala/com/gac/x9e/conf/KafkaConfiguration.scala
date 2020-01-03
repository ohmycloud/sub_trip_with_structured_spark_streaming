package com.gac.x9e.conf

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Kafka 配置
  */
class KafkaConfiguration extends Serializable{
  private val config:                   Config = ConfigFactory.load()
  lazy    val kafkaConfig:              Config = config.getConfig("kafka")
  lazy    val bootstrapServers:         String = kafkaConfig.getString("common.bootstrap.servers")
}
