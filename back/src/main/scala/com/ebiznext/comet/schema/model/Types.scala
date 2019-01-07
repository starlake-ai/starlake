package com.ebiznext.comet.schema.model

import java.util.regex.Pattern

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.StructField

case class Types(types: List[Type]) {
  def checkValidity(): Either[List[String], Boolean] = {
    val typeNames = types.map(_.name)
    duplicates(typeNames, s"%s is defined %d times. A type can only be defined once.")
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

