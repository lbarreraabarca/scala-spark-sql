package com.data.factory.models

import com.data.factory.exceptions.RequestException

class Payload extends Serializable{

  var inputTables: List[TableSpec] = _
  var outputTable: TableSpec = _
  var query: String = _
  private val stringErrorMessage: String = "%s cannot be null or empty."

  def this(inputTables: List[TableSpec], query: String, outputTable: TableSpec){
    this()
    this.inputTables = inputTables
    this.query = query
    this.outputTable = outputTable
  }

  def isValid(): Boolean =  try {
    inputTables.map(x => x.isValid())
    if(!Option(query).isDefined || query.isEmpty) throw RequestException(stringErrorMessage.format("query"))
    inputTables.map(x => query.contains(x.tableName))
    outputTable.isValid()
  } catch {
    case e: Exception => throw RequestException("%s %s".format(e.getClass, e.getMessage))
  }
}
