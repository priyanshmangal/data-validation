package com.peak.analytics.datavalidation.executor

import com.peak.analytics.datavalidation.configuration.DataValidationGlobalConfiguration
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}



object DataValidationSparkJob {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("data_validation")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    val dataValidationGlobalConfiguration = DataValidationGlobalConfiguration("analytics")

    DataValidationJob(dataValidationGlobalConfiguration).runJob(sqlContext)
  }

}
