package ai.starlake.schema.model

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}

case class ExpectationItem(
  expect: ExpectationItemExpect = ExpectationItemExpect("", ""),
  failOnError: Boolean = true
) {
  def this() = this(
    ExpectationItemExpect("", "")
  ) // Should never be called. Here for Jackson deserialization only
  override def toString: String = s"$expect"

  @JsonIgnore
  def name: String = expect.name

  @JsonIgnore
  def query = expect.query

  @JsonIgnore
  def expected = expect.expected

  @JsonIgnore
  def queryCall(): String = expect.queryCall()
}

@JsonSerialize(using = classOf[ToStringSerializer])
@JsonDeserialize(using = classOf[ExpectationItemExpectDeserializer])
case class ExpectationItemExpect(query: String, expected: String) {
  def this() = this("", "") // Should never be called. Here for Jackson deserialization only
  override def toString: String = s"$query => $expected"

  def name: String = query.replaceAll("[\"']", "").replaceAll("[^a-zA-Z0-9]", "_");

  def queryCall(): String = "{{" + query + "}}"
}

object ExpectationItemExpect {
  def validate(expectationItem: ExpectationItemExpect): Unit = {
    if (expectationItem.query.isEmpty) {
      throw new IllegalArgumentException("query cannot be empty")
    }
    if (expectationItem.expected.isEmpty) {
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
  def apply(expr: String): ExpectationItemExpect = {
    expr.indexOf("=>") match {
      case -1 =>
        throw new IllegalArgumentException(
          s"Invalid expectation format! Got $expr but expected macroCall => expected "
        )
      case i =>
        val macroCall = expr.substring(0, i).trim
        val expected = expr.substring(i + "=>".length).trim
        ExpectationItemExpect(macroCall, expected)
    }
  }
}

class ExpectationItemExpectDeserializer extends JsonDeserializer[ExpectationItemExpect] {
  override def deserialize(jp: JsonParser, ctx: DeserializationContext): ExpectationItemExpect = {
    val value = jp.readValueAs[String](classOf[String])
    ExpectationItemExpect(value)
  }
}
