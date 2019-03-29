package com.ebiznext.comet.schema.handlers
import java.io.File
import java.util.regex.Pattern
import com.ebiznext.comet.schema.model._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ArrayType, StructType}

class InferSchemaHandler(dataframe: DataFrame) {

  private def createAttributes(schema: StructType): List[Attribute] = {
    schema
      .map(
        row =>
          row.dataType.typeName match {
            case "struct" =>
              Attribute(
                row.name,
                row.dataType.typeName,
                Some(false),
                !row.nullable,
                attributes = Some(createAttributes(row.dataType.asInstanceOf[StructType]))
              )
            case "array" =>
              val elemType = row.dataType.asInstanceOf[ArrayType].elementType
              if (elemType.typeName.equals("struct"))
                // if the array contains elements of type struct.
                // {people: [{name:Person1, age:22},{name:Person2, age:25}]}
                Attribute(
                  row.name,
                  elemType.typeName,
                  Some(true),
                  !row.nullable,
                  attributes = Some(createAttributes(elemType.asInstanceOf[StructType]))
                )
              else
                // if it is a regular array. {ages: [21, 25]}
                Attribute(row.name, elemType.typeName, Some(true), !row.nullable)
            case _ => Attribute(row.name, row.dataType.typeName, Some(false), !row.nullable)
        }
      )
      .toList
  }

  def createMetaData(
    mode: String,
    format: String,
    multiline: Boolean,
    array: Boolean,
    withHeader: Boolean,
    separator: String,
    quote: String,
    escape: String,
    write: String,
    partition: Option[Partition] = None,
    index: Boolean,
    mapping: Option[EsMapping] = None
  ): Metadata = {
    Metadata(
      Some(Mode.fromString(mode)),
      Some(Format.fromString(format)),
      Some(multiline),
      Some(array),
      Some(withHeader),
      Some(separator),
      Some(quote),
      Some(escape),
      Some(WriteMode.fromString(write)),
      partition,
      Some(index),
      mapping
    )
  }

  private def createSchema(
    name: String,
    pattern: Pattern,
    attributes: List[Attribute],
    metadata: Metadata,
    merge: Option[MergeOptions],
    comment: Option[String],
    presql: Option[List[String]],
    postsql: Option[List[String]]
  ): Schema = {

    Schema(name, pattern, attributes, Some(metadata), merge, comment, None, None)
  }

  def generateYaml(schema: Schema, savePath:String): Unit = {
    val data: Schema = schema
    val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory)
      .registerModule(DefaultScalaModule)

    mapper.writeValue(new File(savePath), data)
  }

}
