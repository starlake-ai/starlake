package ai.starlake.schema.model

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}

@JsonSerialize(using = classOf[ToStringSerializer])
@JsonDeserialize(using = classOf[ExpectationItemDeserializer])
case class ExpectationItem(query: String, expect: String) {
  def this() = this("", "") // Should never be called. Here for Jackson deserialization only
  override def toString: String = s"$query => $expect"

  def name: String = query.replaceAll("[\"']", "").replaceAll("[^a-zA-Z0-9]", "_");

  def queryCall(): String = "{{" + query + "}}"

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

  /** Parse a string into an ExpectationItem The format is either "macroCall(count) => expected" or
    * "macroCall() => expected" or "macroCall => expected"
    * @param expr:
    *   String to parse
    * @return
    *   ExpectationItem object
    */
  def apply(expr: String): ExpectationItem = {
    expr.indexOf("=>") match {
      case -1 =>
        throw new IllegalArgumentException(
          s"Invalid expectation format! Got $expr but expected macroCall => expected "
        )
      case i =>
        val macroCall = expr.substring(0, i).trim
        val expected = expr.substring(i + "=>".length).trim
        ExpectationItem(macroCall, expected)
    }
  }
}

class ExpectationItemDeserializer extends JsonDeserializer[ExpectationItem] {
  override def deserialize(jp: JsonParser, ctx: DeserializationContext): ExpectationItem = {
    val value = jp.readValueAs[String](classOf[String])
    ExpectationItem(value)
  }
}
