package com.data.factory.models

import com.data.factory.exceptions.TableSpecException
import com.data.factory.utils.FieldValidator
import com.typesafe.scalalogging.Logger

class TableSpec extends Serializable{
  private val log = Logger("TableSpec")
  var tableName: String = _
  var csv: Csv = _
  var parquet: Parquet = _
  var avro: Avro = _
  var s3: S3 = _

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

  def this(tableName: String, avro: Avro) = {
    this(tableName)
    this.tableName = tableName
    this.avro = avro
  }

  def this(tableName: String, s3: S3) = {
    this(tableName)
    this.tableName = tableName
    this.s3 = s3
  }
  def isValid(): Boolean = try {
    val validator = new FieldValidator()
    validator.validStringField("tableName")(tableName)
    if (Option(this.csv).isDefined) this.csv.isValid()
    else if (Option(this.parquet).isDefined) this.parquet.isValid()
    else if (Option(this.avro).isDefined) this.avro.isValid()
    else if (Option(this.s3).isDefined) this.s3.isValid()
    else false
  } catch {
    case e: Exception => throw TableSpecException("%s %s".format(e.getClass, e.getMessage))
  }

  def tableType(): String = {
    if (Option(this.csv).isDefined) "csv"
    else if (Option(this.parquet).isDefined) "parquet"
    else if (Option(this.avro).isDefined) "avro"
    else if (Option(this.s3).isDefined) this.s3.dataType()
    else "undefined"
  }
}
