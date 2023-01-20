package com.data.factory.models

import com.data.factory.utils.FieldValidator

class TableSpec extends Serializable{
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
    validator.validStringField("tableName")(tableName)
    if (this.csv == null) this.parquet.isValid()
    else if (this.parquet == null) this.csv.isValid()
    else this.csv.isValid() || this.parquet.isValid()
  }
}
