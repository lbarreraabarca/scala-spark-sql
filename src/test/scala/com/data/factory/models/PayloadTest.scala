package com.data.factory.models

import com.data.factory.adapters.Base64Encoder
import com.data.factory.exceptions.RequestException
import com.data.factory.ports.Encoder
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
    val encodedQuery = "c2VsZWN0IDEgYXMgZmllbGQgZnJvbSBteUNzdlRhYmxlIGlubmVyIGpvaW4gbXlQYXJxdWV0VGFibGU="
    val outputTable: TableSpec = new TableSpec("output_table", csv)
    val payload = new Payload(List(csvTable, parquetTable), encodedQuery, outputTable)
    assert(payload.isValid)
  }

  it should "throw RequestException when receive an empty query." in {
    val path = "/path/file.csv"
    val delimiter = "|"
    val header = true
    val csv = new Csv(path, delimiter, header)
    val parquet = new Parquet(path)
    val csvTableName = "myCsvTable"
    val parquetTableName = "myParquetTable"
    val csvTable = new TableSpec(csvTableName, csv)
    val parquetTable = new TableSpec(parquetTableName, parquet)
    val encodedQuery = ""
    val outputTable: TableSpec = new TableSpec("output_table", csv)
    val payload = new Payload(List(csvTable, parquetTable), encodedQuery, outputTable)
    val exception = intercept[RequestException] {
      payload.isValid
    }
  }

  it should "return a valid Payload when received a payload from s3 data." in {
    val encodedInput = "ewogICJpbnB1dFRhYmxlcyI6IFsKICAgIHsKICAgICAgInRhYmxlTmFtZSI6ICJjdXN0b21lciIsCiAgICAgICJzMyI6IHsKICAgICAgICAiY3N2IjogewogICAgICAgICAgInBhdGgiOiAiczM6Ly9ia3QtZGF0YS10cmFuc2Zvcm0tOTZjMzc4NGEtNWI4NS9kYXRhL3Jhdy9jdXN0b21lci8iLAogICAgICAgICAgImRlbGltaXRlciI6ICIsIiwKICAgICAgICAgICJoZWFkZXIiOiB0cnVlCiAgICAgICAgfQogICAgICB9CiAgICB9LAogICAgewogICAgICAidGFibGVOYW1lIjogInByb2R1Y3QiLAogICAgICAiczMiOiB7CiAgICAgICAgImNzdiI6IHsKICAgICAgICAgICJwYXRoIjogInMzOi8vYmt0LWRhdGEtdHJhbnNmb3JtLTk2YzM3ODRhLTViODUvZGF0YS9yYXcvcHJvZHVjdC8iLAogICAgICAgICAgImRlbGltaXRlciI6ICJ8IiwKICAgICAgICAgICJoZWFkZXIiOiB0cnVlCiAgICAgICAgfQogICAgICB9CiAgICB9LAogICAgewogICAgICAidGFibGVOYW1lIjogInRpY2tldHMiLAogICAgICAiczMiOiB7CiAgICAgICAgImNzdiI6IHsKICAgICAgICAgICJwYXRoIjogInMzOi8vYmt0LWRhdGEtdHJhbnNmb3JtLTk2YzM3ODRhLTViODUvZGF0YS9yYXcvdGlja2V0cy8iLAogICAgICAgICAgImRlbGltaXRlciI6ICIsIiwKICAgICAgICAgICJoZWFkZXIiOiB0cnVlCiAgICAgICAgfQogICAgICB9CiAgICB9CiAgXSwKICAicXVlcnkiOiAiYzJWc1pXTjBJQW9nSUNBZ2RDNXBaQ0JoY3lCMGFXTnJaWFJmYVdRc0lBb2dJQ0FnWXk1cFpDQmhjeUJqZFhOMGIyMWxjbDlwWkN3Z0NpQWdJQ0JqTG01aGJXVWdZWE1nWTNWemRHOXRaWEpmYm1GdFpTd2dDaUFnSUNCd0xtbGtJR0Z6SUhCeWIyUjFZM1JmYVdRc0lBb2dJQ0FnY0M1dVlXMWxJR0Z6SUhCeWIyUjFZM1JmYm1GdFpTd2dDaUFnSUNCd0xuQnlhV05sSUdGeklIQnliMlIxWTNSZmNISnBZMlVzQ2lBZ0lDQnpkVzBvY0M1d2NtbGpaU2tnYjNabGNpQW9jR0Z5ZEdsMGFXOXVJR0o1SUdNdWFXUXBJR0Z6SUdOMWMzUnZiV1Z5WDNSdmRHRnNYM0J5YVdObENtWnliMjBnZEdsamEyVjBjeUIwSUFwcGJtNWxjaUJxYjJsdUlHTjFjM1J2YldWeUlHTWdiMjRnZEM1amRYTjBiMjFsY2w5cFpDQTlJR011YVdRZ0NtbHVibVZ5SUdwdmFXNGdjSEp2WkhWamRDQndJRzl1SUhRdWNISnZaSFZqZEY5cFpDQTlJSEF1YVdRPSIsCiAgIm91dHB1dFRhYmxlIjogewogICAgICAidGFibGVOYW1lIjogInRpY2tldHNfYnlfY3VzdG9tZXIiLAogICAgICAiczMiOiB7CiAgICAgICAgInBhcnF1ZXQiOiB7CiAgICAgICAgICAicGF0aCI6ICJzMzovL2JrdC1kYXRhLXRyYW5zZm9ybS05NmMzNzg0YS01Yjg1L2RhdGEvdGlja2V0c19ieV9jdXN0b21lciIKICAgICAgICB9CiAgICAgIH0KICB9Cn0="
    val encoder: Encoder = new Base64Encoder()
    val decodedInput = encoder.decode(encodedInput)
    implicit val formats = Serialization.formats(NoTypeHints)
    val request: Payload = read[Payload](decodedInput)
    assert(request.isValid())
  }
}
