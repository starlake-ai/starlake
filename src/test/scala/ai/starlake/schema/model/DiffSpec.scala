package ai.starlake.schema.model

import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DiffSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with StrictLogging {

  "Metadata diff on exact same objects" should "return empty list" in {
    val me = Metadata(
      format = Some(Format.DSV),
      encoding = Some("UTF8"),
      multiline = None,
      array = None,
      withHeader = None,
      separator = None,
      quote = None,
      escape = None,
      sink = None,
      ignore = None,
      directory = None,
      ack = None,
      options = None,
      loader = None,
      emptyIsNull = None,
      dagRef = None,
      freshness = None,
      nullValue = None,
      fillWithDefaultValue = true,
      schedule = None,
      writeStrategy = None
    )
    val you = Metadata(
      format = Some(Format.DSV),
      encoding = Some("UTF8"),
      multiline = None,
      array = None,
      withHeader = None,
      separator = None,
      quote = None,
      escape = None,
      sink = None,
      ignore = None,
      directory = None,
      ack = None,
      options = None,
      loader = None,
      emptyIsNull = None,
      dagRef = None,
      freshness = None,
      nullValue = None,
      fillWithDefaultValue = true,
      schedule = None,
      writeStrategy = None
    )
    val result = me.compare(you)
    println(result)
    assert(result.isEmpty())
  }

  "Metadata diff on differents objects" should "return set of diffs" in {
    val me = Metadata(
      format = None,
      encoding = Some("UTF16"),
      multiline = Some(false),
      array = None,
      withHeader = None,
      separator = Some(","),
      quote = None,
      escape = Some("\\"),
      sink = None,
      ignore = None,
      directory = None,
      ack = None,
      options = Some(Map("opt1" -> "value1")),
      loader = None,
      emptyIsNull = None,
      dagRef = None,
      freshness = None,
      nullValue = None,
      fillWithDefaultValue = true,
      schedule = None,
      writeStrategy = None
    )
    val you = Metadata(
      format = Some(Format.DSV),
      encoding = Some("UTF8"),
      multiline = None,
      array = Some(true),
      withHeader = None,
      separator = Some(";"),
      quote = Some("'"),
      escape = None,
      sink = None,
      ignore = None,
      directory = None,
      ack = None,
      options = Some(Map("opt1" -> "value1", "opt2" -> "value2")),
      loader = None,
      emptyIsNull = None,
      dagRef = None,
      freshness = Some(Freshness(Some("1d"), Some("2d"))),
      nullValue = None,
      fillWithDefaultValue = true,
      schedule = None,
      writeStrategy = None
    )
    val result = me.compare(you)
    println(result)
    assert(result.updated.length == 9)
    assert(result.added.isEmpty)
    assert(result.deleted.isEmpty)
  }
}
