package com.gac.x9e.conf

import com.typesafe.config.{Config, ConfigFactory}

class SocketConfiguration {
  private val config:  Config = ConfigFactory.load()
  lazy val socketConf: Config = config.getConfig("socket")
  lazy val host:       String = socketConf.getString("host")
  lazy val port:       Int    = socketConf.getInt("port")
}
