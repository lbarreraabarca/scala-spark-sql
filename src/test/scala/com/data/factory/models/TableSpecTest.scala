package com.data.factory.models

import com.data.factory.exceptions.TableSpecException
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

  it should "create a valid object when receive a valid avro and tableName." in {
    val path = "/path/file.avro"
    val tableName = "myTable"
    val avro = new Avro(path)

    val table = new TableSpec(tableName, avro)

    assert(table.isValid())
    assert(table.avro.path == path)
  }

  it should "create a valid object when receive a valid s3 and tableName." in {
    val path = "s3://path/file/parquet"
    val tableName = "myTable"
    val parquet = new Parquet(path)
    val s3 = new S3(parquet)

    val table = new TableSpec(tableName, s3)

    assert(table.isValid())
    assert(table.s3.parquet.path == path)
  }

  it should "throw TableSpecException when tableName is null." in {
    val path = "/path/file.parquet"
    val tableName = null
    val parquet = new Parquet(path)

    val table = new TableSpec(tableName, parquet)
    val exception = intercept[TableSpecException] {
      table.isValid()
    }
  }

  "tableType" should "return csv when receive a valid csv object." in {
    val path = "/path/file.csv"
    val delimiter = "|"
    val header = true
    val tableName = "myTable"
    val csv = new Csv(path, delimiter, header)

    val table = new TableSpec(tableName, csv)
    assert("csv" == table.tableType())
  }

  it should "return parquet when receive a valid parquet object." in {
    val path = "/path/file.parquet"
    val parquet = new Parquet(path)
    val tableName = "myTable"

    val table = new TableSpec(tableName, parquet)
    assert("parquet" == table.tableType())
  }

  it should "return avro when receive a valid avro object." in {
    val path = "/path/file.avro"
    val tableName = "myTable"
    val avro = new Avro(path)

    val table = new TableSpec(tableName, avro)
    assert("avro" == table.tableType())
  }

  it should "return csv when receive a valid s3 object using csv files." in {
    val path = "s3://path/file.avro"
    val tableName = "myTable"
    val csv = new Csv(path, "|", true)
    val s3 = new S3(csv)

    val table = new TableSpec(tableName, s3)
    assert("csv" == table.tableType())
  }

  it should "return undefined when receive an invalid object." in {
    val tableName = "myTable"

    val table = new TableSpec(tableName)
    assert("undefined" == table.tableType())
  }
}
