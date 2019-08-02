package com.gac.x9e.conf

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Kafka 配置
  */
class KafkaConfiguration extends Serializable{
  private val config:                   Config = ConfigFactory.load()
  lazy    val kafkaConfig:              Config = config.getConfig("kafka")
  lazy    val bootstrapServers:         String = kafkaConfig.getString("common.bootstrap.servers")
  lazy    val orginNaTopic:             String = kafkaConfig.getString("common.orginNaTopic")       // x9e 国标 kafka topic
  lazy    val orginEnTopic:             String = kafkaConfig.getString("common.orginEnTopic")       // x9e 企标 kafka topic
  lazy    val destinationNaTopic:       String = kafkaConfig.getString("common.destinationNaTopic") // xs6 国标 kafka topic
  lazy    val destinationEnTopic:       String = kafkaConfig.getString("common.destinationEnTopic") // xs6 国标 kafka topic
}
