package com.peak.analytics.datavalidation.executor

import com.peak.analytics.datavalidation.configuration.DataValidationGlobalConfiguration
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._


/** DataValidationJob
 *
 */

class DataValidationJob(dataValidationGlobalConfiguration: DataValidationGlobalConfiguration) extends Serializable {

  // using logger for logging any information of my job execution
  val logger: Logger = LoggerFactory.getLogger(getClass)

  lazy private val jobExecutionTime: String = s"exec-${System.currentTimeMillis()}"

  logger.info("Spark Job Run System Time -" + jobExecutionTime)

  def runJob(sqlContext: SQLContext): Unit = {
    execute(sqlContext)
  }

  /** Execute
   * Data Validation Based On Column Rule Set
   *
   * @param sqlContext : sqlContext
   */
  def execute(sqlContext: SQLContext): Unit = {

    val inputDF = readInputData(sqlContext)

    val primaryKey = dataValidationGlobalConfiguration.ruleSetConfig.getString("primarykey")

    val window = Window.partitionBy(primaryKey)

    val windowPrimaryCountDF = inputDF.withColumn("count", count(primaryKey).over(window))

    val primaryKeyCheckDF = windowPrimaryCountDF.withColumn("errordescription",
      when(col("count") === 1, null).otherwise("primary key validation fail")).drop("count")

    val schemaList = inputDF.schema.toList.map(_.name)

    val finalValidatedDF = schemaList.foldLeft(primaryKeyCheckDF) { case (df, columnName) =>
      df.withColumn("errordescription", validate(col(columnName), col("errordescription"), lit(columnName)))
    }

    val errorDF =  finalValidatedDF.filter(col("errordescription").isNotNull)

    val successDF =  finalValidatedDF.filter(col("errordescription").isNull).drop("errordescription")

    finalValidatedDF.show(false)
    val outputConfig = dataValidationGlobalConfiguration.output
    val outputFilePath = outputConfig.getString("path")

    writeOutputDataFrame(errorDF, outputFilePath+ "/error")
    writeOutputDataFrame(successDF, outputFilePath+ "/success")

  }

  /**
   *
   */
  val validate: UserDefinedFunction = udf { (dataColumn: String, errorColumn: String,
                                             validationCheckColumnName: String) =>

    val columnRuleConfig = dataValidationGlobalConfiguration.columnList.asScala.toList.find { x =>
      x.getString("columnname").toLowerCase() == validationCheckColumnName.toLowerCase
    }

    if (columnRuleConfig.isDefined) {
      val columnRuleSet = columnRuleConfig.get
      val ruleSetConfig = columnRuleSet.getConfig("rules").root().unwrapped().asScala.toList

      val finalErrorMsg = ruleSetConfig.foldLeft(errorColumn) { case (errorMsg, validationCheck) =>
        val validationType = validationCheck._1
        val validationRule = validationCheck._2

        validationType match {
          case "ismandatory" => isMandatory(validationRule.asInstanceOf[Boolean], dataColumn, validationCheckColumnName, errorMsg)
          case "datatype" => dataTypeCheck(validationRule.toString, dataColumn, validationCheckColumnName, errorMsg)
        }

      }
      finalErrorMsg

    } else {
      errorColumn
    }
  }

  /** Check For Mandatory condition i.e null value check. */
  def isMandatory(isMandatory: Boolean, columnValue: String, columnName: String, errorMsg: String): String = {
    if (isMandatory && columnValue == null) {
      val errorReason = s"missing mandatory value for $columnName"
      if (errorMsg == null) {
        errorReason
      } else {
        s"$errorMsg / $errorReason"
      }
    } else {
      errorMsg
    }
  }


  /** DataTypeCheck based on given condition */
  def dataTypeCheck(checkType: String, columnValue: String, columnName: String, errorMsg: String): String = {

    val errorReason = checkType match {
      case "varchar(255)" => isVarChar(columnValue, columnName)
      case "integer" => isInteger(columnValue, columnName)
      case _ if checkType.contains("decimal") => isDecimal(columnValue, columnName)
    }

    if (errorMsg == null) {
      errorReason
    } else if (errorReason != null) {
      s"$errorMsg / $errorReason"
    } else {
      errorMsg
    }

  }

  def isVarChar(columnValue: String, columnName: String): String = {
    if (columnValue.isInstanceOf[String] && columnValue.length <= 255) {
      null
    } else {
      s"$columnName is not of type varchar(255)"
    }
  }

  def isInteger(columnValue: String, columnName: String): String = {
    try {
      columnValue.toInt
      null
    }
    catch {
      case _: NumberFormatException => s"$columnName is not of type Integer Number"
    }

  }

  def isDecimal(columnValue: String, columnName: String): String = {
    try {
      columnValue.toFloat
      null
    }
    catch {
      case _: NumberFormatException => s"$columnName is not of type Decimal Number"
    }

  }


  /** writeOutputDataFrame
   *
   * @param outputDF Output DataFrame
   * @param filePath File Path
   */
  def writeOutputDataFrame(outputDF: DataFrame, filePath: String): Unit = {
    outputDF
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .save(filePath)
  }

  /** readInputData : Read Input Dataframe
   *
   * @param sqlContext SQL Context
   * @return data frame
   */
  def readInputData(sqlContext: SQLContext): DataFrame = {

    val inputConfig = dataValidationGlobalConfiguration.input
    val filePath = inputConfig.getString("path")
    val inferSchema = if (inputConfig.hasPath("inferSchema")) {
      inputConfig.getBoolean("inferSchema")
    } else {
      false
    }
    val resourceFile = if (inputConfig.hasPath("resource")) {
      inputConfig.getBoolean("resource")
    } else {
      false
    }
    val updatedFilePath = if (resourceFile) {
      getClass.getResource(filePath).getFile
    } else {
      filePath
    }
    val readDF = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", inferSchema)
      .load(updatedFilePath)

    readDF
  }

}

/** Companion Object for DataValidationJob */
object DataValidationJob {
  def apply(dataValidationGlobalConfiguration:
            DataValidationGlobalConfiguration): DataValidationJob
  = new DataValidationJob(dataValidationGlobalConfiguration)
}
