package ai.starlake.semantic

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LookMLConverterSpec extends AnyFlatSpec with Matchers {

  import LookMLConverter.{parseAggregate, sanitize, ParsedAggregate}

  "sanitize" should "lowercase and replace invalid characters" in {
    sanitize("order_id") shouldBe "order_id"
    sanitize("Order Status") shouldBe "order_status"
    sanitize("CUSTOMERS") shouldBe "customers"
    sanitize("weird-name.1") shouldBe "weird_name_1"
  }

  "parseAggregate" should "map simple aggregates to native LookML measure types" in {
    parseAggregate("SUM(order_total)") shouldBe Some(ParsedAggregate("sum", Some("order_total")))
    parseAggregate("avg(order_total)") shouldBe Some(
      ParsedAggregate("average", Some("order_total"))
    )
    parseAggregate("MIN(x)") shouldBe Some(ParsedAggregate("min", Some("x")))
    parseAggregate("MAX(x)") shouldBe Some(ParsedAggregate("max", Some("x")))
    parseAggregate("COUNT(*)") shouldBe Some(ParsedAggregate("count", None))
    parseAggregate("COUNT(DISTINCT customers.customer_id)") shouldBe Some(
      ParsedAggregate("count_distinct", Some("customers.customer_id"))
    )
  }

  it should "reject expressions that are not a single simple aggregate" in {
    parseAggregate("SUM(a)/NULLIF(SUM(b),0)") shouldBe None
    parseAggregate("SUM(a) + SUM(b)") shouldBe None
    parseAggregate("COUNT(order_id)") shouldBe None
    parseAggregate("CASE WHEN x THEN 1 ELSE 0 END") shouldBe None
    parseAggregate("order_total") shouldBe None
  }
}
