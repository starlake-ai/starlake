package ai.starlake.core.utils

import ai.starlake.schema.model.Materialization.TABLE
import ai.starlake.schema.model.Trim.BOTH
import ai.starlake.schema.model.{
  AccessControlEntry,
  AllSinks,
  Attribute,
  DomainInfo,
  ExpectationItem,
  Format,
  Freshness,
  Metadata,
  Position,
  RowLevelSecurity,
  SchemaInfo,
  WriteStrategy
}
import com.hubspot.jinjava.Jinjava
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

class CaseClassToPojoConverterSpec extends AnyFlatSpec {

  "asJava" should "convert domain and table succesfully" in {
    val javaItem = CaseClassToPojoConverter.asJava(
      DomainInfo(
        "aDomain",
        metadata = Some(
          Metadata(
            format = Some(Format.POSITION),
            encoding = Some("UTF-8"),
            multiline = Some(true),
            sink = Some(
              AllSinks(
                materializedView = Some(TABLE)
              )
            ),
            options = Some(Map("opt1" -> "optValue1")),
            freshness = Some(Freshness(warn = Some(Duration.create(2, TimeUnit.DAYS).toString()))),
            fillWithDefaultValue = false,
            writeStrategy = Some(WriteStrategy.Overwrite)
          )
        ),
        tables = List(
          SchemaInfo(
            name = "table",
            pattern = Pattern.compile("table.*\\.pos"),
            attributes = List(
              Attribute(
                name = "firstname",
                position = Some(Position(first = 1, last = 3)),
                trim = Some(BOTH)
              )
            ),
            rls = List(RowLevelSecurity(name = "top-secret")),
            expectations = List(ExpectationItem(expect = "is true")),
            acl = List(AccessControlEntry(role = "dev"))
          )
        ),
        tags = Set("energy")
      )
    )
    val jinjava = new Jinjava()
    val context = Map.from(List("rawDomain" -> javaItem)).asJava
    val template =
      """domain:{{rawDomain.name}}
        |domain.metadata.format:{{rawDomain.metadata.format}}
        |domain.metadata.encoding:{{rawDomain.metadata.encoding}}
        |domain.metadata.directory:{{rawDomain.metadata.directory}}
        |domain.metadata.multiline:{{rawDomain.metadata.multiline}}
        |domain.metadata.array:{{rawDomain.metadata.array}}
        |domain.metadata.sink.materializedView:{{rawDomain.metadata.sink.materializedView}}
        |domain.metadata.options:{{rawDomain.metadata.options}}
        |domain.metadata.options.opt1:{{rawDomain.metadata.options.opt1}}
        |domain.metadata.freshness.warn:{{rawDomain.metadata.freshness.warn}}
        |domain.metadata.fillWithDefaultValue:{{rawDomain.metadata.fillWithDefaultValue}}
        |domain.metadata.writeStrategy.type:{{rawDomain.metadata.writeStrategy.type}}
        |domain.tables[0].pattern:{{rawDomain.tables[0].pattern}}
        |domain.tables[0].attributes[0].position.first:{{rawDomain.tables[0].attributes[0].position.first}}
        |domain.tables[0].attributes[0].trim:{{rawDomain.tables[0].attributes[0].trim}}
        |domain.tables[0].rls[0].name:{{rawDomain.tables[0].rls[0].name}}
        |domain.tables[0].expectations[0].expect:{{rawDomain.tables[0].expectations[0].expect}}
        |domain.tables[0].acl[0].role:{{rawDomain.tables[0].acl[0].role}}
        |domain.tags[0]:{{rawDomain.tags[0]}}
        |""".stripMargin

    val renderedTemplate = jinjava.render(template, context)
    println(renderedTemplate)
    renderedTemplate shouldBe """domain:aDomain
                                |domain.metadata.format:{value=POSITION}
                                |domain.metadata.encoding:UTF-8
                                |domain.metadata.directory:
                                |domain.metadata.multiline:true
                                |domain.metadata.array:
                                |domain.metadata.sink.materializedView:{value=TABLE}
                                |domain.metadata.options:{opt1=optValue1}
                                |domain.metadata.options.opt1:optValue1
                                |domain.metadata.freshness.warn:2 days
                                |domain.metadata.fillWithDefaultValue:false
                                |domain.metadata.writeStrategy.type:{value=OVERWRITE}
                                |domain.tables[0].pattern:table.*\.pos
                                |domain.tables[0].attributes[0].position.first:1
                                |domain.tables[0].attributes[0].trim:{value=BOTH}
                                |domain.tables[0].rls[0].name:top-secret
                                |domain.tables[0].expectations[0].expect:is true
                                |domain.tables[0].acl[0].role:dev
                                |domain.tags[0]:energy
                                |""".stripMargin
  }
}
