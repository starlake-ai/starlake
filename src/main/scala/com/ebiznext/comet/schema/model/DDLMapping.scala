package com.ebiznext.comet.schema.model

trait DDLField {
  def toMap(): Map[String, Any]
}
case class DDLLeaf(
  name: String,
  tpe: String,
  required: Boolean,
  comment: Option[String],
  primaryKey: Boolean
) extends DDLField {
  override def toMap(): Map[String, Any] = {
    Map(
      "nodeType"   -> "leaf",
      "name"       -> name,
      "type"       -> tpe,
      "required"   -> required.toString,
      "comment"    -> comment.getOrElse(""),
      "primaryKey" -> primaryKey.toString
    )
  }
}

case class DDLNode(name: String, fields: List[DDLField], required: Boolean, comment: Option[String])
    extends DDLField {
  override def toMap(): Map[String, Any] = {
    Map(
      "type"     -> "node",
      "name"     -> name,
      "fields"   -> fields.map(_.toMap()),
      "required" -> required.toString,
      "comment"  -> comment.getOrElse("")
    )
  }
}
