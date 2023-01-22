package com.data.factory.models

import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.read
import org.scalatest._
class PayloadTest extends FlatSpec {

  "constructor" should "create a valid object when receive valid attributes" in {
    val path = "/path/file.csv"
    val delimiter = "|"
    val header = true
    val csv = new Csv(path, delimiter, header)
    val parquet = new Parquet(path)
    val csvTableName = "myCsvTable"
    val parquetTableName = "myParquetTable"
    val csvTable = new TableSpec(csvTableName, csv)
    val parquetTable = new TableSpec(parquetTableName, parquet)
    val encodedQuery = "c2VsZWN0IDEgYXMgZmllbGQ="
    val outputTable: TableSpec = new TableSpec("output_table", csv)
    val payload = new Payload(List(csvTable, parquetTable), encodedQuery, outputTable)
    assert(payload.isValid)
  }

}
