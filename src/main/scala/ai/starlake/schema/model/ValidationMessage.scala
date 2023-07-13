package ai.starlake.schema.model

case class ValidationMessage(severity: Severity, target: String, message: String) {
  override def toString(): String = {
    s"$severity: $target: $message"
  }
}
