package org.rabbit.config

import java.util.Properties


trait DbConfig {

  val user: String
  val password: String
  val url: String
  val driverClass: String

  def pop: Properties = {
    val pop = new Properties()
    pop.setProperty("user", user)
    pop.setProperty("password", password)
    pop.setProperty("driver", driverClass)
    pop
  }

}

object PrestoConfig extends DbConfig {
  val url: String = FileConfig.prestoDataSource.getString("url")

  val user: String = FileConfig.prestoDataSource.getString("user")
  val password: String = FileConfig.prestoDataSource.getString("password")
  val driverClass: String = FileConfig.prestoDataSource.getString("driverClass")
}

