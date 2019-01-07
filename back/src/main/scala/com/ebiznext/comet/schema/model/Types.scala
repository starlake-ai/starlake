package com.ebiznext.comet.schema.model

import java.util.regex.Pattern

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.StructField

import scala.collection.mutable

case class Types(types: List[Type]) {
  def checkValidity(): Either[List[String], Boolean] = {
    val errorList: mutable.MutableList[String] = mutable.MutableList.empty
    val typeNames = types.map(_.name)
    val duplicates = typeNames.groupBy(identity).mapValues(_.size).filter {
      case (key, size) => size > 1
    }
    duplicates.foreach { case (key, size) =>
      errorList += s"$key is defined $size times. A type can only be defined once."
    }
    if (errorList.nonEmpty)
      Left(errorList.toList)
    else
      Right(true)
  }
}

case class Type(name: String, pattern: Pattern, primitiveType: PrimitiveType = PrimitiveType.string) {
  def matches(value: String): Boolean = {
    pattern.matcher(name).matches()
  }

  def sparkType(fieldName: String, nullable: Boolean, comment: Option[String]): StructField = {
    StructField(fieldName, CatalystSqlParser.parseDataType(primitiveType.value), nullable).withComment(comment.getOrElse(""))
  }
}

