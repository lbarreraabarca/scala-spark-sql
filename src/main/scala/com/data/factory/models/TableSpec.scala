package com.data.factory.models

import com.data.factory.utils.FieldValidator
import com.typesafe.scalalogging.Logger

class TableSpec extends Serializable{
  private val log = Logger("TableSpec")
  var tableName: String = _
  var csv: Csv = _
  var parquet: Parquet = _

  def this(tableName: String) = {
    this()
    this.tableName = tableName
  }

  def this(tableName: String, csv: Csv) = {
    this(tableName)
    this.csv = csv
  }

  def this(tableName: String, parquet: Parquet) = {
    this(tableName)
    this.tableName = tableName
    this.parquet = parquet
  }
  def isValid(): Boolean = {
    val validator = new FieldValidator()
    log.info("isValid tableName %s".format(tableName))
    log.info("isValid Option csv %s".format(Option(this.csv).isDefined))
    log.info("isValid Option parquet %s".format(Option(this.parquet).isDefined))
    validator.validStringField("tableName")(tableName)
    if (Option(this.csv).isDefined) this.csv.isValid()
    else if (Option(this.parquet).isDefined) this.parquet.isValid()
    else false
  }

  def tableType(): String = {
    log.info("tableType TableName %s".format(tableName))
    log.info("tableType Option csv %s".format(Option(this.csv).isDefined))
    log.info("tableType Option parquet %s".format(Option(this.parquet).isDefined))
    if (Option(this.csv).isDefined) "csv"
    else if (Option(this.parquet).isDefined) "parquet"
    else "undefined"
  }
}
