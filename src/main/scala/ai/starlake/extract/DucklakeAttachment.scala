package ai.starlake.extract

object DucklakeAttachment {
  case class AttachValues(metadataLocation: String, dbName: String, dataPathValue: String)
  def extract(inputString: String): Option[AttachValues] = {
    val pattern =
      "\\s*(?:==>\\s*)?ATTACH\\s+IF\\s+NOT\\s+EXISTS\\s+'ducklake:([^']+?)'\\s+AS\\s+([^\\s(]+?)\\s*\\(\\s*DATA_PATH\\s+([^)]+?)\\s*\\)\\s*;?\\s*$".r
    inputString.trim match {
      case pattern(value1, value2, value3) =>
        Some(AttachValues(value1.trim, value2.trim, value3.trim))
      case _ =>
        None // Return None if the string does not match the expected pattern
    }
  }
}
