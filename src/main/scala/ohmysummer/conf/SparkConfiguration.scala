package ohmysummer.conf

import com.typesafe.config.ConfigFactory

/**
  * Spark 配置信息
  */
class SparkConfiguration extends Serializable {
  private val config = ConfigFactory.load()
  lazy val sparkConf = config.getConfig("spark")
  lazy val sparkMaster                 = sparkConf.getString("master")
  lazy val sparkUIEnabled              = sparkConf.getBoolean("ui.enabled")
  lazy val sparkStreamingBatchDuration = sparkConf.getLong("streaming.batch.duration")
  lazy val checkPointPath              = sparkConf.getString("checkpoint.path")
  lazy val stopperPort                 = sparkConf.getInt("stopper.port")
  lazy val ttl                         = sparkConf.getString("spark.cleaner.ttl")
  lazy val cleanCheckpoints            = sparkConf.getString("spark.cleaner.referenceTracking.cleanCheckpoints")
}
