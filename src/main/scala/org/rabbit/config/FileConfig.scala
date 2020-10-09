package org.rabbit.config

import java.io.File

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}

object FileConfig {

  private val fileConf = ConfigFactory.parseFile(new File("./application.conf"))
  private val online = ConfigFactory.parseResourcesAnySyntax("online")
  private val local = ConfigFactory.parseResourcesAnySyntax("local")
  private val develop = ConfigFactory.parseResourcesAnySyntax("application")
  private val reference = ConfigFactory.parseResourcesAnySyntax("app")
  private val default = ConfigFactory.load() //default environment

  //global config
  val myConfig: Config = fileConf.withFallback(online).withFallback(local).withFallback(develop).withFallback(reference)
  val combinedConfig: Config = myConfig.withFallback(default)

  //library or custom config

  val prestoDataSource: Config = combinedConfig.getConfig("presto")

  printConf(myConfig)
  def printConf(config: Config): Unit = println(config.root().render(ConfigRenderOptions.concise().setFormatted(true).setJson(true)))

}
