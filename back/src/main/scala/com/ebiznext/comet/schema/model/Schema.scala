package com.ebiznext.comet.schema.model

import java.util.regex.Pattern

import scala.collection.mutable

/**
  * Dataset Schema
  *
  * @param name       : Schema name, must be unique in the domain. Will become the hive table name
  * @param pattern    : filename pattern to which this schema must be applied
  * @param attributes : datasets columns
  * @param metadata   : Dataset metadata
  * @param comment    : free text
  * @param presql     :  SQL code executed before the file is ingested
  * @param postsql    : SQL code executed right after the file has been ingested
  */
case class Schema(name: String,
                  pattern: Pattern,
                  attributes: List[Attribute],
                  metadata: Option[Metadata],
                  comment: Option[String],
                  presql: Option[List[String]],
                  postsql: Option[List[String]]
                 ) {
  /**
    * @return Are the parittions columns defined in the metadata valid column names
    */
  def validatePartitionColumns(): Boolean = {
    metadata.forall(_.getPartition().forall(attributes.map(_.getFinalName()).union(Metadata.CometPartitionColumns).contains))
  }


  /**
    * return the list of renamed attributes
    * @return list of tuples (oldname, newname)
    */
  def renamedAttributes(): List[(String, String)] = {
    attributes.map(attr => (attr.name, attr.getFinalName())).filter {
      case (name, finalName) => name != finalName
    }
  }

  /**
    * Check attribute definition correctness :
    *   - schema name should be a valid table identifier
    *   - attribute name should be a valid Hive column identifier
    *   - attribute name can occur only once in the schema
    *
    * @param types : List of globally defined types
    * @return error list of true
    */
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

