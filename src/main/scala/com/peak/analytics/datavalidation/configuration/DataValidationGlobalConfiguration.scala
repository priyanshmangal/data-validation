package com.peak.analytics.datavalidation.configuration

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}


class DataValidationGlobalConfiguration(config: Config) extends Serializable {

  val input: Config = config.getConfig("input")
  val output: Config = config.getConfig("output")

  private val ruleSet = config.getConfig("ruleset")
  private val ruleSetPath = ruleSet.getString("path")
  private val ruleSetFile = new File(ruleSetPath)

  val ruleSetConfig: Config = ConfigFactory.parseFile(ruleSetFile)

  val columnList =  ruleSetConfig.getConfigList("columns")

}

object DataValidationGlobalConfiguration {
  def apply(source: String): DataValidationGlobalConfiguration = {
    val analyticsConfig = ConfigFactory.load(source)
    new DataValidationGlobalConfiguration(analyticsConfig)
  }
}


