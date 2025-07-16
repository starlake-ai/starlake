package ai.starlake.schema.model

import ai.starlake.transpiler.diff.Attribute as DiffAttribute

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

  def toDiffAttribute(): DiffAttribute = {
    new DiffAttribute(this.name, this.`type`)
  }
}
