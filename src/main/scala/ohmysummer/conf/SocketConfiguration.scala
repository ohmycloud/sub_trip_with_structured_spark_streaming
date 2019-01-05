package ohmysummer.conf

import com.typesafe.config.ConfigFactory

class SocketConfiguration {
  private val config = ConfigFactory.load()
  lazy val socketConf = config.getConfig("socket")

  lazy val host = socketConf.getString("host")
  lazy val port = socketConf.getInt("port")
}
