package com.ebiznext.comet.schema.handlers

import java.util.regex.Pattern

import com.ebiznext.comet.config.DatasetArea
import com.ebiznext.comet.schema.model.SchemaModel
import com.ebiznext.comet.schema.model.SchemaModel.{Domain, Schema, Type, Types}
import org.apache.hadoop.fs.Path
import org.json4s.Formats
import org.json4s.native.Serialization.{read => jsread, write => jswrite}

 class SchemaHandler(storage: StorageHandler) {
  implicit val formats: Formats = SchemaModel.formats

  lazy val types: Types = {
    val typesPath = new Path(DatasetArea.types, "types.json")
    jsread[Types](storage.read(typesPath))
  }


  def getType(tpe: String): Type = {
    types.types.find(_.name == tpe).getOrElse(throw new Exception(s"Invalid Type $tpe found"))
  }

  lazy val domains: List[Domain] = {
    storage
      .list(DatasetArea.domains, ".json")
      .map(path => jsread[Domain](storage.read(path)))

  }

  def getDomain(name: String): Option[Domain] = {
    domains.find(_.name == name)
  }

  def getSchemas(domain: String): List[Schema] = {
    getDomain(domain).map(_.schemas).getOrElse(Nil)
  }

  def getSchema(domainName: String, schemaName: String): Option[Schema] = {
    for {
      domain <- getDomain(domainName)
      schema <- domain.schemas.find(_.name == schemaName)
    } yield schema
  }
  def getTypes(domainName: String, schemaName: String): List[Type] = {
    val types = for {
      domain <- getDomain(domainName).toList
      schema <- domain.schemas.find(_.name == schemaName).toList
      attribute <- schema.attributes
    } yield getType(attribute.`type`)
    types
  }
}
