# Data crossing using spark SQL

This application solve the use case when we have different files types such as csv, parquet or avro and we want to make data cross between them.
This service allows read csv, parquet or avro files and apply a 
spark sql transform through a query. Then, this result will store in csv or parquet files.


## Pre requirements
You need to have installed the followings tools:

- `java 11`
- `mvn`
- `spark`

## Payload
When you run your application, you need to define your payload. This must be Base64 encoded and it must have the following fields.
Also `query` field must be Base64 encoded. Some constraints, if you define a table in `query`, it's defined as an `inputTables[*].tableName`. 

```json
{
  "inputTables": [
    {
      "tableName": "customer",
      "csv": {
        "path": "customer-local-path",
        "delimiter": ",",
        "header": true
      }
    },
    {
      "tableName": "product",
      "csv": {
        "path": "product-local-path",
        "delimiter": "|",
        "header": true
      }
    },
    {
      "tableName": "tickets-local-path",
      "csv": {
        "path": "",
        "delimiter": ",",
        "header": true
      }
    }
  ],
  "query": "c2VsZWN0IAogICAgdC5pZCBhcyB0aWNrZXRfaWQsIAogICAgYy5pZCBhcyBjdXN0b21lcl9pZCwgCiAgICBjLm5hbWUgYXMgY3VzdG9tZXJfbmFtZSwgCiAgICBwLmlkIGFzIHByb2R1Y3RfaWQsIAogICAgcC5uYW1lIGFzIHByb2R1Y3RfbmFtZSwgCiAgICBwLnByaWNlIGFzIHByb2R1Y3RfcHJpY2UsCiAgICBzdW0ocC5wcmljZSkgb3ZlciAocGFydGl0aW9uIGJ5IGMuaWQpIGFzIGN1c3RvbWVyX3RvdGFsX3ByaWNlCmZyb20gdGlja2V0cyB0IAppbm5lciBqb2luIGN1c3RvbWVyIGMgb24gdC5jdXN0b21lcl9pZCA9IGMuaWQgCmlubmVyIGpvaW4gcHJvZHVjdCBwIG9uIHQucHJvZHVjdF9pZCA9IHAuaWQ=",
  "outputTable": {
      "tableName": "result",
      "csv": {
        "path": "output-path",
        "delimiter": ",",
        "header": true
      }
  }
}
```
## Payload using AWS S3 data
You can read and write data from `AWS S3` buckets. Data type supported are `csv` and `parquet` files.
```json
{
  "inputTables": [
    {
      "tableName": "customer",
      "s3": {
        "csv": {
          "path": "s3://bucket-name/customer-data-path/",
          "delimiter": ",",
          "header": true
        }
      }
    },
    {
      "tableName": "product",
      "s3": {
        "csv": {
          "path": "s3://bucket-name/product-data-path/",
          "delimiter": ",",
          "header": true
        }
      }
    },
    {
      "tableName": "tickets",
      "s3": {
        "csv": {
          "path": "s3://bucket-name/tickets-data-path/",
          "delimiter": ",",
          "header": true
        }
      }
    }
  ],
  "query": "c2VsZWN0IAogICAgdC5pZCBhcyB0aWNrZXRfaWQsIAogICAgYy5pZCBhcyBjdXN0b21lcl9pZCwgCiAgICBjLm5hbWUgYXMgY3VzdG9tZXJfbmFtZSwgCiAgICBwLmlkIGFzIHByb2R1Y3RfaWQsIAogICAgcC5uYW1lIGFzIHByb2R1Y3RfbmFtZSwgCiAgICBwLnByaWNlIGFzIHByb2R1Y3RfcHJpY2UsCiAgICBzdW0ocC5wcmljZSkgb3ZlciAocGFydGl0aW9uIGJ5IGMuaWQpIGFzIGN1c3RvbWVyX3RvdGFsX3ByaWNlCmZyb20gdGlja2V0cyB0IAppbm5lciBqb2luIGN1c3RvbWVyIGMgb24gdC5jdXN0b21lcl9pZCA9IGMuaWQgCmlubmVyIGpvaW4gcHJvZHVjdCBwIG9uIHQucHJvZHVjdF9pZCA9IHAuaWQ=",
  "outputTable": {
      "tableName": "tickets_by_customer",
      "s3": {
        "parquet": {
          "path": "s3://bucket-name/data-transformed-path-tickets_by_customer"
        }
      }
  }
}
```

## How to use?
### Local run
You must compile the jar and run the application.
```bash
mvn test
mvn clean package
spark-submit --class com.data.factory.App <local-path>\scala-spark-sql-1.0-SNAPSHOT-jar-with-dependencies.jar <encodedRequest>
```

### AWS run
- You can to run this service over EMR serverless. You need:
- Create a EMR Serverless application
- Create jar and upload in AWS S3 path.
- Run a spark job over EMR serverless application.
