package com.ebiznext.comet.schema.model

import java.util.regex.Pattern

import better.files.File

import scala.collection.mutable

/**
  * Let's say you are wiling to import from you Sales system customers and orders.
  * Sales is therefore the domain and cusomer & order are syour datasets.
  * @param name : Domain name
  * @param directory : Folder on the local filesystem where incomping files are stored.
  *                  This folder will be scanned regurlaly to move the dataset to the cluster
  * @param metadata : Default Schema meta data.
  * @param schemas : List of schema for each dataset in this domain
  * @param comment : Free text
  * @param extensions : recognized filename extensions (json, csv, dsv, psv) are recognized by default
  * @param ack : Ack extension used for each file
  */
case class Domain(name: String,
                  directory: String,
                  metadata: Option[Metadata] = None,
                  schemas: List[Schema] = Nil,
                  comment: Option[String] = None,
                  extensions: Option[List[String]] = None,
                  ack: Option[String] = None
                 ) {
  /**
    * Get schema from filename
    * Schema are matched against filenames using filename patterns.
    * The schema pattern thats matches the filename is returned
    * @param filename : dataset filename
    * @return
    */
  def findSchema(filename: String): Option[Schema] = {
    schemas.find(_.pattern.matcher(filename).matches())
  }

  /**
    * List of file extensions to scan for in the domain directory
    * @return the list of extensions of teh default ones : ".json", ".csv", ".dsv", ".psv"
    */
  def getExtensions(): List[String] = {
    extensions.getOrElse(List("json", "csv", "dsv", "psv")).map("." + _)
  }

  /**
    * Ack file should be present for each file to ingest.
    * @return the ack attribute or ".ack" by default
    */
  def getAck(): String = ack.map("." + _).getOrElse(".ack")


  /**
    * Is this Domain valid ? A domain is valid if :
    *   - The domain name is a valid attribute
    *   - all the schemas defined in this domain are valid
    *   - No schema is defined twice
    *   - Partitions columns are valid columns
    *   - The input directory is a valid path
    * @param types
    * @return
    */
  def checkValidity(types: List[Type]): Either[List[String], Boolean] = {
    val errorList: mutable.MutableList[String] = mutable.MutableList.empty

    // Check Domain name validity
    val dbNamePattern = Pattern.compile("[a-zA-Z][a-zA-Z0-9_]{1,100}")
    if (!dbNamePattern.matcher(name).matches())
      errorList += s"Schema with name $name should respect the pattern ${dbNamePattern.pattern()}"

    // Check Schema validity
    schemas.foreach { schema =>
      for (errors <- schema.checkValidity(types).left) {
        errorList ++= errors
      }
    }

    val duplicatesErrorMessage = "%s is defined %d times. A schema can only be defined once."
    for (errors <- duplicates(schemas.map(_.name), duplicatesErrorMessage).left) {
      errorList ++= errors
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
