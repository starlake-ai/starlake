package ai.starlake.schema.model

import ai.starlake.transpiler.schema.JdbcColumn

import java.sql.Types

/** A field in the schema. For struct fields, the field "attributes" contains all sub attributes
  *
  * @param name
  *   : Attribute name as defined in the source dataset and as received in the file
  * @param comment
  *   : free text for attribute description
  */
case class AutoTaskAttribute(
  name: String,
  `type`: String = "variant",
  comment: String = "",
  accessPolicy: Option[String] = None,
  foreignKey: Option[String] = None // [domain.]table.attribute
) {
  def this() = this("") // Should never be called. Here for Jackson deserialization only

  override def toString: String = {
    s"AutoTaskAttribute(name=$name, type=${`type`}, comment=$comment, accessPolicy=$accessPolicy)"
  }

  def toJdbcColumn(database: String, domain: String, table: String): JdbcColumn = {
    new JdbcColumn(database, domain, table, name, Types.OTHER, `type`, 0, 0, 0, "", null)
  }
}
