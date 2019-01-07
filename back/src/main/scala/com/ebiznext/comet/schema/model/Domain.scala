package com.ebiznext.comet.schema.model

import java.util.regex.Pattern

import better.files.File

import scala.collection.mutable

case class Domain(name: String,
                  directory: String,
                  metadata: Option[Metadata] = None,
                  schemas: List[Schema] = Nil,
                  comment: Option[String] = None,
                  extensions: Option[List[String]] = None,
                  ack: Option[String] = None
                 ) {
  def findSchema(filename: String): Option[Schema] = {
    schemas.find(_.pattern.matcher(filename).matches())
  }

  def getExtensions(): List[String] = {
    extensions.getOrElse(List("json", "csv", "dsv", "psv")).map("." + _)
  }

  def getAck(): String = ack.map("." + _).getOrElse(".ack")

  def checkValidity(types: Types): Either[List[String], Boolean] = {
    val errorList: mutable.MutableList[String] = mutable.MutableList.empty

    // Check Domain name validity
    val dbNamePattern = Pattern.compile("[a-zA-Z][a-zA-Z0-9_]{1,100}")
    if (!dbNamePattern.matcher(name).matches())
      errorList += s"Schema with name $name should respect the pattern ${dbNamePattern.pattern()}"

    // Check Schema validity
    schemas.foreach { schema =>
      schema.checkValidity(types) match {
        case Left(errors) => errorList ++= errors
      }
    }

    // Check Schema name unicity
    val duplicates = schemas.map(_.name).groupBy(identity).mapValues(_.size).filter {
      case (key, size) => size > 1
    }
    duplicates.foreach { case (key, size) =>
      errorList += s"$key is defined $size times. A schema can only be defined once."
    }

    // TODO Check partition columns

    // TODO Validate directory
    val inputDir = File(this.directory)
    if (!inputDir.exists) {
      errorList += s"$directory not found"
    }
    if (errorList.nonEmpty)
      Left(errorList.toList)
    else
      Right(true)
  }
}
