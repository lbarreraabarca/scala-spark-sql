package com.data.factory.ports

import com.data.factory.models.TableSpec
import org.apache.spark.sql.DataFrame

trait DataOperator {
  def getTable(tableSpec: TableSpec): DataFrame
  def sql(query: String): DataFrame
  def saveTable(tableSpec: TableSpec, dataFrame: DataFrame): Unit

}
