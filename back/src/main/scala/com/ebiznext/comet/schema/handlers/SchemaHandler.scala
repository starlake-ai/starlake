package com.ebiznext.comet.schema.handlers

import com.ebiznext.comet.config.DatasetArea
import com.ebiznext.comet.schema.model.SchemaModel.{Domain, Schema, Type, Types}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.fs.Path



class SchemaHandler(storage: StorageHandler) {
  // uses Jackson YAML to parsing, relies on SnakeYAML for low level handling
  val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
  mapper.registerModule(DefaultScalaModule)

 lazy val types: Types = {
   val typesPath = new Path(DatasetArea.types, "types.yml")
   mapper.readValue(storage.read(typesPath), classOf[Types])
 }


 def getType(tpe: String): Type = {
   types.types.find(_.name == tpe).getOrElse(throw new Exception(s"Invalid Type $tpe found"))
 }

 lazy val domains: List[Domain] = {
   storage
     .list(DatasetArea.domains, ".yml")
     .map(path => mapper.readValue(storage.read(path), classOf[Domain]))

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
