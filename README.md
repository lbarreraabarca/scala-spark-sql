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

## How to use?
You must compile the jar and run the application.
```bash
mvn test
mvn clean package
spark-submit --class com.data.factory.App <local-path>\scala-spark-sql-1.0-SNAPSHOT-jar-with-dependencies.jar <encodedRequest>
```

## Another functions

### [AWS] Read data from S3 and write in S3 bucket.
```bash
ewogICJpbnB1dFRhYmxlcyI6IFsKICAgIHsKICAgICAgInRhYmxlTmFtZSI6ICJjdXN0b21lciIsCiAgICAgICJzMyI6IHsKICAgICAgICAiY3N2IjogewogICAgICAgICAgInBhdGgiOiAiczM6Ly9ia3QtZGF0YS10cmFuc2Zvcm0tOTZjMzc4NGEtNWI4NS9kYXRhL3Jhdy9jdXN0b21lci8iLAogICAgICAgICAgImRlbGltaXRlciI6ICIsIiwKICAgICAgICAgICJoZWFkZXIiOiB0cnVlCiAgICAgICAgfQogICAgICB9CiAgICB9LAogICAgewogICAgICAidGFibGVOYW1lIjogInByb2R1Y3QiLAogICAgICAiczMiOiB7CiAgICAgICAgImNzdiI6IHsKICAgICAgICAgICJwYXRoIjogInMzOi8vYmt0LWRhdGEtdHJhbnNmb3JtLTk2YzM3ODRhLTViODUvZGF0YS9yYXcvcHJvZHVjdC8iLAogICAgICAgICAgImRlbGltaXRlciI6ICIsIiwKICAgICAgICAgICJoZWFkZXIiOiB0cnVlCiAgICAgICAgfQogICAgICB9CiAgICB9LAogICAgewogICAgICAidGFibGVOYW1lIjogInRpY2tldHMiLAogICAgICAiczMiOiB7CiAgICAgICAgImNzdiI6IHsKICAgICAgICAgICJwYXRoIjogInMzOi8vYmt0LWRhdGEtdHJhbnNmb3JtLTk2YzM3ODRhLTViODUvZGF0YS9yYXcvdGlja2V0cy8iLAogICAgICAgICAgImRlbGltaXRlciI6ICIsIiwKICAgICAgICAgICJoZWFkZXIiOiB0cnVlCiAgICAgICAgfQogICAgICB9CiAgICB9CiAgXSwKICAicXVlcnkiOiAiYzJWc1pXTjBJQW9nSUNBZ2RDNXBaQ0JoY3lCMGFXTnJaWFJmYVdRc0lBb2dJQ0FnWXk1cFpDQmhjeUJqZFhOMGIyMWxjbDlwWkN3Z0NpQWdJQ0JqTG01aGJXVWdZWE1nWTNWemRHOXRaWEpmYm1GdFpTd2dDaUFnSUNCd0xtbGtJR0Z6SUhCeWIyUjFZM1JmYVdRc0lBb2dJQ0FnY0M1dVlXMWxJR0Z6SUhCeWIyUjFZM1JmYm1GdFpTd2dDaUFnSUNCd0xuQnlhV05sSUdGeklIQnliMlIxWTNSZmNISnBZMlVzQ2lBZ0lDQnpkVzBvY0M1d2NtbGpaU2tnYjNabGNpQW9jR0Z5ZEdsMGFXOXVJR0o1SUdNdWFXUXBJR0Z6SUdOMWMzUnZiV1Z5WDNSdmRHRnNYM0J5YVdObENtWnliMjBnZEdsamEyVjBjeUIwSUFwcGJtNWxjaUJxYjJsdUlHTjFjM1J2YldWeUlHTWdiMjRnZEM1amRYTjBiMjFsY2w5cFpDQTlJR011YVdRZ0NtbHVibVZ5SUdwdmFXNGdjSEp2WkhWamRDQndJRzl1SUhRdWNISnZaSFZqZEY5cFpDQTlJSEF1YVdRPSIsCiAgIm91dHB1dFRhYmxlIjogewogICAgICAidGFibGVOYW1lIjogInRpY2tldHNfYnlfY3VzdG9tZXIiLAogICAgICAiczMiOiB7CiAgICAgICAgInBhcnF1ZXQiOiB7CiAgICAgICAgICAicGF0aCI6ICJzMzovL2JrdC1kYXRhLXRyYW5zZm9ybS05NmMzNzg0YS01Yjg1L2RhdGEvdGlja2V0c19ieV9jdXN0b21lciIKICAgICAgICB9CiAgICAgIH0KICB9Cn0=
```