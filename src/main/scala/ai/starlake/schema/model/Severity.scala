package ai.starlake.schema.model

sealed abstract class Severity(value: String)

object Severity {
  def fromString(value: String): Severity = {
    value.toUpperCase() match {
      case "ERROR"    => Error
      case "WARNING"  => Warning
      case "INFO"     => Info
      case "DISABLED" => Disabled
      case _          => Disabled
    }
  }
  final object Error extends Severity("ERROR")
  final object Warning extends Severity("WARNING")
  final object Info extends Severity("INFO")
  final object Disabled extends Severity("DISABLED")

}
