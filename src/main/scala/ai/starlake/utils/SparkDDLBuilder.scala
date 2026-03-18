package ai.starlake.utils

import ai.starlake.extract.JdbcDbUtils
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.types.{StructField, StructType}

/** Pure SQL DDL string generation methods extracted from SparkUtils. These methods generate SQL
  * strings but do not execute them.
  */
object SparkDDLBuilder extends LazyLogging {

  def added(incoming: StructType, existing: StructType): StructType = {
    val incomingFields = incoming.fields.map(_.name).toSet
    val existingFields = existing.fields.map(_.name.toLowerCase()).toSet
    val newFields = incomingFields.filter(f => !existingFields.contains(f.toLowerCase()))
    val fields = incoming.fields.filter(f => newFields.contains(f.name))
    StructType(fields)
  }

  def dropped(incoming: StructType, existing: StructType): StructType = {
    val incomingFields = incoming.fields.map(_.name.toLowerCase()).toSet
    val existingFields = existing.fields.map(_.name).toSet
    val deletedFields = existingFields.filter(f => !incomingFields.contains(f.toLowerCase()))
    val fields = existing.fields.filter(f => deletedFields.contains(f.name))
    StructType(fields)
  }

  def alterTableDropColumnsString(
    engineName: String,
    fields: StructType,
    tableName: String
  ): Seq[String] = {
    val dropFields = fields.map(_.name)
    val ifExists = if (engineName.toLowerCase() == "redshift") "" else "IF EXISTS"
    dropFields.map(dropColumn => s"ALTER TABLE $tableName DROP COLUMN $ifExists $dropColumn")
  }

  def alterTableAddColumnsString(
    engineName: String,
    allFields: StructType,
    tableName: String,
    attributesWithDDLType: Map[String, String]
  ): Seq[String] = {
    allFields.fields
      .flatMap(alterTableAddColumnString(engineName, _, tableName, attributesWithDDLType))
      .toIndexedSeq
  }

  def alterTableAddColumnString(
    engineName: String,
    field: StructField,
    tableName: String,
    attributesWithDDLType: Map[String, String]
  ): Option[String] = {
    val addField = field.name
    val addFieldType = field.dataType

    val (jdbcType, isArray) = JdbcDbUtils.getCommonJDBCType(addFieldType)
    val typeStr = jdbcType.map(_.databaseTypeDefinition)
    val withArray =
      if (isArray) {
        engineName match {
          case "snowflake" => typeStr.map(_ => "ARRAY")
          case "duckdb"    => typeStr.map(_ + "[]")
          case _           => typeStr
        }
      } else
        typeStr
    val addJdbcType =
      attributesWithDDLType
        .get(addField)
        .orElse(withArray)

    val nullable = ""
    val ifNotExists =
      if (engineName.toLowerCase() == "redshift") "" else "IF NOT EXISTS"
    addJdbcType.map(jdbcType =>
      s"ALTER TABLE $tableName ADD COLUMN $ifNotExists $addField $jdbcType $nullable"
    )
  }

  def isFlat(fields: StructType): Boolean = {
    val deep = fields.fields.exists(_.dataType.isInstanceOf[StructType])
    !deep
  }
}
