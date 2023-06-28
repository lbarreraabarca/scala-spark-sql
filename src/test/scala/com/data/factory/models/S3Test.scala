package com.data.factory.models


import com.data.factory.exceptions.{FieldValidatorException, RequestException}
import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.read
import org.scalatest._
class S3Test extends FlatSpec {

  // CSV variables
  val path = "s3://path/file"
  val delimiter = "|"
  val header = true
  val csv = new Csv(path, delimiter, header)

  // Parquet variables
  val parquet = new Parquet(path)

  "constructor" should "create a valid s3 object when receive a valid csv" in {
    val s3 = new S3(csv)
    assert(s3.isValid())
  }

  it should "create a valid s3 object when receive a valid parquet" in {
    val s3 = new S3(parquet)
    assert(s3.isValid())
  }

  it should "throw an exception when receive an invalid csv." in {
    val fakeCsv = new Csv(null, delimiter, header)
    val exception = intercept[RequestException] {
      new S3(fakeCsv)
    }
    val errorMessage = "class com.data.factory.exceptions.RequestException class com.data.factory.exceptions.FieldValidatorException path cannot be null or empty."
    assert(errorMessage.equals(exception.getMessage))
  }

  it should "throw an exception when receive an invalid parquet." in {
    val fakeParquet = new Parquet(null)
    val exception = intercept[RequestException] {
      new S3(fakeParquet)
    }
  }

  "isValid" should "throw an exception when receive an invalid parquet path." in {
    val fakeParquet = new Parquet("invalid")
    val s3 = new S3(fakeParquet)
    val exception = intercept[RequestException] {
      s3.isValid()
    }
  }

  it should "throw an exception when receive an invalid csv path." in {
    val fakeCsv = new Csv("invalid", delimiter, header)
    val s3 = new S3(fakeCsv)
    val exception = intercept[RequestException] {
      s3.isValid()
    }
  }

  "dataType" should "return a csv data type." in {
    val s3 = new S3(csv)
    assert(s3.dataType() == "csv")
  }

  "dataType" should "return a parquet data type." in {
    val s3 = new S3(parquet)
    assert(s3.dataType() == "parquet")
  }
}
