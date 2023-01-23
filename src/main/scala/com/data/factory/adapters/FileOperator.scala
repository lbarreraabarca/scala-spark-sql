package com.data.factory.adapters

import com.data.factory.exceptions.DataOperatorException
import com.data.factory.models.TableSpec
import com.data.factory.ports.DataOperator
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class FileOperator extends DataOperator with Serializable {
  private val log = Logger("FileOperator")
  private var session: SparkSession = _
  private val objectErrorMessage: String = "%s cannot be null."
  private val stringErrorMessage: String = "%s cannot be null or empty."

  def this(session: SparkSession){
    this()
    if (session == null) throw DataOperatorException(objectErrorMessage.format("session"))
    else this.session = session
  }

  override def getTable(tableSpec: TableSpec): DataFrame = {
    if(tableSpec.tableName == null || tableSpec.tableName.isEmpty) throw DataOperatorException(stringErrorMessage.format("tableName"))

    log.info("Loading table %s that is %s table type".format(tableSpec.tableName, tableSpec.tableType))
    log.info("%s".format(tableSpec.csv.path))
    tableSpec.tableType() match {
      case "csv" =>
        log.info("Loading a csv file")
        val dataFrame: DataFrame = this.session.read
          .option("delimiter", tableSpec.csv.delimiter)
          .option("header", "true"/*tableSpec.csv.header.toString*/)
          .csv(tableSpec.csv.path)
        dataFrame.createOrReplaceTempView(tableSpec.tableName)
        dataFrame
      case "parquet" =>
        val dataFrame = this.session.read.parquet(tableSpec.parquet.path)
        dataFrame.createOrReplaceTempView(tableSpec.tableName)
        dataFrame
      case _ => throw DataOperatorException("Invalid format file %s".format(tableSpec.tableType))
    }
  }

  override def sql(query: String): DataFrame = {
    if(query == null || query.isEmpty) throw DataOperatorException(stringErrorMessage.format("query"))
    else this.session.sql(query)
  }

  override def saveTable(tableSpec: TableSpec, dataFrame: DataFrame): Unit = {
    if(tableSpec.tableName == null || tableSpec.tableName.isEmpty) throw DataOperatorException(stringErrorMessage.format("tableName"))
    if(dataFrame == null) throw DataOperatorException(objectErrorMessage.format("dataFrame"))
    else
      tableSpec.tableType() match {
      case "csv" => dataFrame.write
        .mode(SaveMode.Overwrite)
        .option("delimiter", tableSpec.csv.delimiter)
        .option("header", tableSpec.csv.header.toString)
        .csv(tableSpec.csv.path)
      case "parquet" => dataFrame.write
        .mode(SaveMode.Overwrite)
        .parquet(tableSpec.parquet.path)
      case _ => throw DataOperatorException("Invalid format file %s".format(tableSpec.tableType))
    }

  }
}
