package com.ebiznext.comet.schema.handlers
import java.io.File
import java.util.regex.Pattern

import com.ebiznext.comet.schema.model._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ArrayType, StructType}

class InferSchemaHandler(dataframe:DataFrame, header:Boolean) {

  private def createAttributes(schema: StructType):List[Attribute] ={
    schema.map(row => row.dataType.typeName match {
      case "struct" => Attribute(row.name, row.dataType.typeName, Some(false), !row.nullable, attributes= Some(createAttributes(row.dataType.asInstanceOf[StructType])))
      case "array" =>
        val elemType = row.dataType.asInstanceOf[ArrayType].elementType
        if(elemType.typeName.equals("struct"))
        // if the array contains elements of type struct.
        // {people: [{name:Person1, age:22},{name:Person2, age:25}]}
          Attribute(row.name, elemType.typeName, Some(true),!row.nullable,attributes= Some(createAttributes(elemType.asInstanceOf[StructType])))
        else
        // if it is a regular array. {ages: [21, 25]}
          Attribute(row.name, elemType.typeName, Some(true),!row.nullable)
      case _ => Attribute(row.name, row.dataType.typeName,Some(false) ,!row.nullable)
    }).toList
  }


  private def createMetaData():Metadata ={
      Metadata(Some(Mode.FILE),Some(Format.JSON), Some(true),Some(false), Some(header) )
    }


  private def createSchema(p: Pattern ):Schema ={
    Schema("Test",p, createAttributes(dataframe.schema), Some(createMetaData), None, None, None, None)

  }

  def generateYaml(savePath: String): Unit ={
    val data: Schema = createSchema(Pattern) // todo this wont work for now.
    val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory)
      .registerModule(DefaultScalaModule)

    mapper.writeValue(new File(savePath), data)
  }

}
