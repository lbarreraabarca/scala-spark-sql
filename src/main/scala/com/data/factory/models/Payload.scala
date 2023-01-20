package com.data.factory.models

class Payload extends Serializable{

  var inputTables: List[TableSpec] = _

  def this(inputTables: List[TableSpec]){
    this()
    this.inputTables = inputTables
  }

  def isValid(): Boolean = inputTables.map(x => x.isValid()).contains(false)
}
