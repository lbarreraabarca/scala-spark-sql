package com.data.factory.utils

import com.data.factory.exceptions.FieldValidatorException

import scala.util.matching.Regex

class FieldValidator {
  private val invalidField: String = "%s cannot be null or empty."

  val validField: String => String => Regex => Boolean = field => fieldName => matcher =>
    field match {
      case matcher(_*) => true
      case _ => throw FieldValidatorException(invalidField.format(fieldName))
    }

  val validStringField: String => String => Boolean = fieldName => fieldValue =>
    if (fieldValue == null || fieldValue.isEmpty) throw FieldValidatorException(invalidField.format(fieldName))
    else !(fieldValue == null || fieldValue.isEmpty)

  val validBoolean: String => Boolean => Boolean = fieldName => fieldValue =>
    if (fieldValue == null) throw FieldValidatorException(invalidField.format(fieldName))
    else !(fieldValue == null)

}
