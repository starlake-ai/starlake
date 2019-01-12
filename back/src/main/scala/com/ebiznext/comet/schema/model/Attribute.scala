package com.ebiznext.comet.schema.model

import java.util.regex.Pattern

import scala.collection.mutable

/**
  * A field in the schema. For struct fields, the field "attributes" contains all sub attributes
  *
  * @param name       : Attribute name as defined in the source dataset
  * @param `type`     : semantic type of the attribute
  * @param required   : Should this attribute always be present in the source
  * @param privacy    : Shoudl this attribute be applied a privacy transformaiton at ingestion time
  * @param comment    : free text for attribute description
  * @param rename     : If present, the attribute is renamed with this name
  * @param attributes : List of sub-attributes
  */
case class Attribute(name: String,
                     `type`: String = "string",
                     required: Boolean = true,
                     privacy: PrivacyLevel = PrivacyLevel.NONE,
                     comment: Option[String] = None,
                     rename: Option[String] = None,
                     attributes: Option[List[Attribute]] = None
                    ) {
  /**
    * Check attribute validity
    * An attribute is valid if :
    *     - Its name is a valid identifier
    *     - its type is defined
    *     - When a privacy function is defined its primitive type is a string
    *
    * @param types : List of defined types.
    * @return true if attribute is valid
    */
  def checkValidity(types: Types): Either[List[String], Boolean] = {
    val errorList: mutable.MutableList[String] = mutable.MutableList.empty

    val colNamePattern = Pattern.compile("[a-zA-Z][a-zA-Z0-9_]{1,767}")
    if (!colNamePattern.matcher(name).matches())
      errorList += s"attribute with name $name should respect the pattern ${colNamePattern.pattern()}"

    if (!rename.forall(colNamePattern.matcher(_).matches()))
      errorList += s"renamed attribute with renamed name '$rename' should respect the pattern ${colNamePattern.pattern()}"

    val primitiveType = types.types.find(_.name == `type`).map(_.primitiveType)
    primitiveType match {
      case None => errorList += s"Invalid Type ${`type`}"
      case Some(tpe) if tpe != PrimitiveType.string && privacy != PrivacyLevel.NONE =>
        errorList += s"string is the only supported primitive type for an attribute when privacy is requested"
    }

    if (errorList.nonEmpty)
      Left(errorList.toList)
    else
      Right(true)
  }

  /**
    * @return renamed column if defined, source name otherwise
    */
  def getFinalName(): String = rename.getOrElse(name)
}
