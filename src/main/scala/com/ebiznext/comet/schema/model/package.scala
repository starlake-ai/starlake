package com.ebiznext.comet.schema

import scala.collection.mutable

package object model {

  /**
    * Utility to extract duplicates and their number of occurrences
    *
    * @param values       : Liste of strings
    * @param errorMessage : Error Message that should contains placeholders for the value(%s) and number of occurrences (%d)
    * @return List of tuples contains for ea  ch duplicate the number of occurrences
    */
  def duplicates(values: List[String], errorMessage: String): Either[List[String], Boolean] = {
    val errorList: mutable.MutableList[String] = mutable.MutableList.empty
    val duplicates = values.groupBy(identity).mapValues(_.size).filter {
      case (key, size) => size > 1
    }
    duplicates.foreach {
      case (key, size) =>
        errorList += errorMessage.format(key, size)
    }
    if (errorList.nonEmpty)
      Left(errorList.toList)
    else
      Right(true)
  }

  def combine(
    errors1: Either[List[String], Boolean],
    errors2: Either[List[String], Boolean]*
  ): Either[List[String], Boolean] = {
    val allErrors = errors1 :: List(errors2: _*)
    val errors = allErrors.collect {
      case Left(err) => err
    }.flatten
    if (errors.isEmpty) Right(true) else Left(errors)
  }
}
