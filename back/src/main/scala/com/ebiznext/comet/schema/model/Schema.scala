package com.ebiznext.comet.schema.model

import java.util.regex.Pattern

import scala.collection.mutable

case class Schema(name: String,
                  pattern: Pattern,
                  attributes: List[Attribute],
                  metadata: Option[Metadata],
                  comment: Option[String],
                  presql: Option[List[String]],
                  postsql: Option[List[String]]
                 ) {
  def validatePartitionColumns(): Boolean = {
    metadata.forall(_.getPartition().forall(attributes.map(_.name).union(Metadata.CometPartitionColumns).contains))
  }

  def renamedAttributes(): List[(String, String)] = {
    attributes.filter(_.rename.isDefined).map(attribute => (attribute.name, attribute.rename.get))
  }

  def checkValidity(types: Types): Either[List[String], Boolean] = {
    val errorList: mutable.MutableList[String] = mutable.MutableList.empty
    val tableNamePattern = Pattern.compile("[a-zA-Z][a-zA-Z0-9_]{1,256}")
    if (!tableNamePattern.matcher(name).matches())
      errorList += s"Schema with name $name should respect the pattern ${tableNamePattern.pattern()}"

    attributes.foreach { attribute =>
      for (errors <- attribute.checkValidity(types).left) {
        errorList ++= errors
      }
    }

    val duplicateErrorMessage = "%s is defined %d times. An attribute can only be defined once."
    for (errors <- duplicates(attributes.map(_.name), duplicateErrorMessage).left) {
      errorList ++= errors
    }

    if (errorList.nonEmpty)
      Left(errorList.toList)
    else
      Right(true)
  }
}

