package com.data.factory.models

import com.data.factory.exceptions.RequestException
class S3 extends Serializable {

  var csv: Csv = _
  var parquet: Parquet = _

  def this(csv: Csv){
    this()
    try{
      csv.isValid()
      this.csv = csv
    } catch {
      case e: Exception => throw RequestException("%s %s".format(e.getClass, e.getMessage))
    }
  }

  def this(parquet: Parquet) {
    this()
    try {
      parquet.isValid()
      this.parquet = parquet
    } catch {
      case e: Exception => throw RequestException("%s %s".format(e.getClass, e.getMessage))
    }
  }

  def isValid(): Boolean = try {
    if (Option(this.csv).isDefined) {
      this.csv.isValid()
      if(!this.csv.path.startsWith("s3://")) throw RequestException("csv.path must start with s3://. You've registered %s".format(this.csv.path))
      else true
    }
    else if (Option(this.parquet).isDefined) {
      this.parquet.isValid()
      if(!this.parquet.path.startsWith("s3://")) throw RequestException("parquet.path must start with s3://. You've registered %s".format(this.parquet.path))
      else true
    }
    else throw RequestException("Invalid S3 object.")
  } catch {
    case e: Exception => throw RequestException("%s %s".format(e.getClass, e.getMessage))
  }

  def dataType(): String = {
    if (Option(this.csv).isDefined) "csv"
    else if (Option(this.parquet).isDefined) "parquet"
    else "undefined"
  }
}
