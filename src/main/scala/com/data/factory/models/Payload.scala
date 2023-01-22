package com.data.factory.models

class Payload extends Serializable{

  var inputTables: List[TableSpec] = _
  var outputTable: TableSpec = _
  var query: String = _

  def this(inputTables: List[TableSpec], query: String, outputTable: TableSpec){
    this()
    this.inputTables = inputTables
    this.query = query
    this.outputTable = outputTable
  }

  def isValid(): Boolean =
    ! inputTables.map(x => x.isValid()).contains(false) && outputTable.isValid() && !(query == null) && !(query.isEmpty)
}
