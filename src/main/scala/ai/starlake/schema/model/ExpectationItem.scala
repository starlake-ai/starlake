package ai.starlake.schema.model

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}

@JsonSerialize(using = classOf[ToStringSerializer])
@JsonDeserialize(using = classOf[ExpectationItemDeserializer])
case class ExpectationItem(query: String, expect: String, name: Option[String] = None) {
  def this() = this("", "") // Should never be called. Here for Jackson deserialization only
  override def toString: String = s"$query => $expect"

  def queryCall() = "{{" + query + "}}"

}

object ExpectationItem {
  def validate(expectationItem: ExpectationItem): Unit = {
    if (expectationItem.query.isEmpty) {
      throw new IllegalArgumentException("query cannot be empty")
    }
    if (expectationItem.expect.isEmpty) {
      throw new IllegalArgumentException("expect cannot be empty")
    }
  }

  def apply(expr: String): ExpectationItem = {
    expr.indexOf("=>") match {
      case -1 =>
        expr.indexOf(')') match {
          case -1 => ExpectationItem(expr, "")
          case i =>
            ExpectationItem(expr.substring(0, i + 1).trim, "count " + expr.substring(i + 2).trim)
        }
        ExpectationItem(expr, "")
      case i =>
        val macroCall = expr.substring(0, i).trim
        val expected = expr.substring(i + 2).trim
        val parIndex = macroCall.indexOf("(")
        val macroName =
          if (parIndex > 0)
            macroCall.substring(0, parIndex)
          else
            ""
        ExpectationItem(macroCall, expected, Some(macroName))
    }
  }
}

class ExpectationItemDeserializer extends JsonDeserializer[ExpectationItem] {
  override def deserialize(jp: JsonParser, ctx: DeserializationContext): ExpectationItem = {
    val value = jp.readValueAs[String](classOf[String])
    ExpectationItem(value)
  }
}
