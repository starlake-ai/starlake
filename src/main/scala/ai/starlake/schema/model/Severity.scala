package ai.starlake.schema.model

import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer

@JsonSerialize(using = classOf[ToStringSerializer])
sealed abstract class Severity(value: String) {
  override def toString: String = String.format("%1$-8s", value)

}

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

  def main(args: Array[String]): Unit = {
    println(Severity.fromString("ERROR"))
    println(Severity.fromString("WARNING"))
    println(Severity.fromString("INFO"))
    println(Severity.fromString("DISABLED"))
    println(Severity.fromString("UNKNOWN"))
  }
}
