package com.gac.x9e.conf

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Spark 配置
  */
class SparkConfiguration {
  private val config:      Config = ConfigFactory.load()
  lazy    val sparkConf:   Config = config.getConfig("spark")
  lazy    val sparkMaster: String = sparkConf.getString("common.master")
  lazy    val checkpoint:  String = sparkConf.getString("common.checkpoint")
  lazy    val logLevel:    String = sparkConf.getString("common.log.level")
  lazy    val appName:     String = sparkConf.getString("common.app.name")
  lazy    val sparkInterval: String = sparkConf.getString("common.trigger.interval")
}
