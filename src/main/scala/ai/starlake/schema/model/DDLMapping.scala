package ai.starlake.schema.model

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

case class DDLNode(
  name: String,
  fields: List[DDLField],
  required: Boolean,
  isArray: Boolean,
  comment: Option[String]
) extends DDLField {
  override def toMap(): Map[String, Any] = {
    val tpe = if (isArray) "ARRAY" else "STRUCT"
    Map(
      "nodeType" -> "node",
      "name"     -> name,
      "type"     -> tpe,
      "fields"   -> fields.map(_.toMap()),
      "required" -> required.toString,
      "comment"  -> comment.getOrElse("")
    )
  }
}
