package com.peak.analytics.datavalidation.executor

import com.peak.analytics.datavalidation.configuration.DataValidationGlobalConfiguration
import com.peak.analytics.datavalidation.utility.sharedcontext.SharedSparkContext
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class DataValidationJobTest extends FunSuite with BeforeAndAfterEach with SharedSparkContext {

  val applicationConfig = ConfigFactory.parseResources("analytics.conf")
  val dataValidationGlobalConfiguration = new DataValidationGlobalConfiguration(applicationConfig)

  test("DataValidationJobTest - Jun Job") {

    DataValidationJob(dataValidationGlobalConfiguration).runJob(sqlContext)

  }

  test("DataValidationJobTest - execute") {

    DataValidationJob(dataValidationGlobalConfiguration).execute(sqlContext)

  }
}
