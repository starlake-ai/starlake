package com.ebiznext.comet.schema.model

import java.util.regex.Pattern

import scala.collection.mutable

case class Attribute(name: String,
                     `type`: String = "string",
                     required: Boolean = true,
                     privacy: PrivacyLevel = PrivacyLevel.NONE,
                     comment: Option[String] = None,
                     rename: Option[String] = None,
                     attributes: Option[List[Attribute]] = None
                    ) {
  def checkValidity(types: Types): Either[List[String], Boolean] = {
    val errorList: mutable.MutableList[String] = mutable.MutableList.empty

    val colNamePattern = Pattern.compile("[a-zA-Z][a-zA-Z0-9_]{1,767}")
    if (!colNamePattern.matcher(name).matches())
      errorList += s"attribute with name $name should respect the pattern ${colNamePattern.pattern()}"

    val primitiveType = types.types.find(_.name == `type`).map(_.primitiveType)
    primitiveType match {
      case None => errorList += s"Invalid Type ${`type`}"
      case Some(tpe) if tpe != PrimitiveType.string =>
        errorList += s"string is the only supported primitive type for an attribute when privacy is requested"
    }

    if (errorList.nonEmpty)
      Left(errorList.toList)
    else
      Right(true)
  }
}
