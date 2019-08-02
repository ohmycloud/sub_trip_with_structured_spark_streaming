package com.gac.x9e.conf

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Spark 配置
  */
class SparkConfiguration {
  private val config:                      Config = ConfigFactory.load()
  lazy    val sparkConf:                   Config = config.getConfig("spark")
  lazy    val sparkMaster:                 String = sparkConf.getString("common.master")

  lazy    val NaTriggerDuration:              Int = sparkConf.getInt("nation.streaming.trigger.duration")
  lazy    val NaCheckPointPath:            String = sparkConf.getString("nation.checkpoint.path")
  lazy    val NaTtl:                       String = sparkConf.getString("nation.spark.cleaner.ttl")
  lazy    val NaLogCleanupDelay:           Int    = sparkConf.getInt("nation.fileSink.log.cleanupDelay")

  lazy    val EnTriggerDuration:              Int = sparkConf.getInt("enterprise.streaming.trigger.duration")
  lazy    val EnCheckPointPath:            String = sparkConf.getString("enterprise.checkpoint.path")
  lazy    val EnTtl:                       String = sparkConf.getString("enterprise.spark.cleaner.ttl")
  lazy    val EnLogCleanupDelay:           Int    = sparkConf.getInt("enterprise.fileSink.log.cleanupDelay")
}
