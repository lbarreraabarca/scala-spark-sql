package com.data.factory.adapters

import com.data.factory.exceptions.DataOperatorException
import com.data.factory.models.TableSpec
import com.data.factory.ports.DataOperator
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

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
    var dataFrame: DataFrame = null
    log.info("Loading table %s that is %s table type".format(tableSpec.tableName, tableSpec.tableType))
    log.info("%s".format(tableSpec.csv.path))
    tableSpec.tableType() match {
      case "csv" => dataFrame = this.session.read
          .option("delimiter", tableSpec.csv.delimiter)
          .option("header", tableSpec.csv.header.toString)
          .csv(tableSpec.csv.path)
      case "parquet" => dataFrame = this.session.read.parquet(tableSpec.parquet.path)
      case _ => throw DataOperatorException("Invalid format file %s".format(tableSpec.tableType))
    }
    dataFrame.createOrReplaceTempView(tableSpec.tableName)
    dataFrame
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
        .option("delimiter", tableSpec.csv.delimiter)
        .option("header", tableSpec.csv.header.toString)
        .csv(tableSpec.csv.path)
      case "parquet" => dataFrame.write.parquet(tableSpec.parquet.path)
      case _ => throw DataOperatorException("Invalid format file %s".format(tableSpec.tableType))
    }

  }
}
