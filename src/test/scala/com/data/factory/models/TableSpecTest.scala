package com.data.factory.models

import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.read
import org.scalatest._

class TableSpecTest extends FlatSpec{

  "constructor" should "create a valid object when receive a valid csv and tableName." in {
    val path = "/path/file.csv"
    val delimiter = "|"
    val header = true
    val tableName = "myTable"
    val csv = new Csv(path, delimiter, header)

    val table = new TableSpec(tableName, csv)

    assert(table.isValid())
    assert(table.csv.path == path)
    assert(table.csv.delimiter == delimiter)
    assert(table.csv.header == header)

  }

  it should "create a valid object when receive a valid parquet and tableName." in {
    val path = "/path/file.parquet"
    val tableName = "myTable"
    val parquet = new Parquet(path)

    val table = new TableSpec(tableName, parquet)

    assert(table.isValid())
    assert(table.parquet.path == path)
  }
}
