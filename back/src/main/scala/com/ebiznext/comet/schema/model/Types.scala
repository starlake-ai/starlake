package com.ebiznext.comet.schema.model

import java.util.regex.Pattern

import org.apache.spark.sql.types.StructField

/**
  * List of globally defined types
  *
  * @param types : Type list
  */
case class Types(types: List[Type]) {
  def checkValidity(): Either[List[String], Boolean] = {
    val typeNames = types.map(_.name)
    duplicates(typeNames, s"%s is defined %d times. A type can only be defined once.")
  }
}

/**
  * Semantic Type
  *
  * @param name          : Type name
  * @param pattern       : Pattern use to check that the input data matches the pattern
  * @param primitiveType : Spark Column Type of the attribute
  */
case class Type(name: String, pattern: Pattern, primitiveType: PrimitiveType = PrimitiveType.string, sample : Option[String] = None, comment:Option[String] = None) {
  def matches(value: String): Boolean = {
    pattern.matcher(name).matches()
  }

  def sparkType(fieldName: String, nullable: Boolean, comment: Option[String]): StructField = {
    StructField(fieldName, primitiveType.sparkType, nullable).withComment(comment.getOrElse(""))
  }


}

