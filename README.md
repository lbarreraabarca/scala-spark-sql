# Spark SQL Transform

This service allows read csv or parquet files and apply a 
spark sql transform. Then, this dataframe will store in csv or parquet files.

## Request
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

```bash
mvn test
mvn clean package
spark-submit --class com.data.factory.App <local-path>\scala-spark-sql-1.0-SNAPSHOT-jar-with-dependencies.jar <encodedRequest>
```