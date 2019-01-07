package com.ebiznext.comet.schema

import scala.collection.mutable

package object model {
  def duplicates(values: List[String], errorMessage: String): Either[List[String], Boolean] = {
    val errorList: mutable.MutableList[String] = mutable.MutableList.empty
    val duplicates = values.groupBy(identity).mapValues(_.size).filter {
      case (key, size) => size > 1
    }
    duplicates.foreach { case (key, size) =>
      errorList += errorMessage.format(key, size)
    }
    if (errorList.nonEmpty)
      Left(errorList.toList)
    else
      Right(true)

  }
}
