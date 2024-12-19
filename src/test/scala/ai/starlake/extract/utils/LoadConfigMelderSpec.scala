package ai.starlake.extract.utils

import ai.starlake.core.utils.{
  Current,
  DomainMelderConfig,
  Extract,
  KeepAll,
  KeepCurrentScript,
  KeepExtract,
  LoadConfigMelder,
  TableAttributeMelderConfig,
  TableMelderConfig
}
import ai.starlake.schema.model.{Attribute, Domain, Metadata, Schema}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.regex.Pattern

class LoadConfigMelderSpec extends AnyFlatSpec with Matchers {

  val sampleAttr1 = Attribute(
    name = "attr1",
    required = Some(true),
    comment = Some("This is attr1"),
    tags = Set("tag1"),
    script = Some("script1"),
    rename = Some("renamedAttr1")
  )

  val currentSampleAttr1 = Attribute(
    name = "attr1",
    required = Some(false),
    comment = Some("This is attr1.2"),
    tags = Set("tag1.2"),
    script = Some("script1.2"),
    rename = Some("renamedAttr1.2")
  )

  val sampleAttr2 = Attribute(name = "attr2", comment = Some("attr2 comment"))

  val defaultDomainMelderConfig = DomainMelderConfig()
  val defaultTableMelderConfig = TableMelderConfig()
  val defaultAttributeConfig = TableAttributeMelderConfig()

  "meldDomain" should "prioritize extracted values when precedence is set to Extract" in {
    val melder = new LoadConfigMelder()
    val sampleExtractedDomain = Domain(
      name = "extractedDomain",
      comment = Some("Extracted Domain Comment"),
      tags = Set("extractTag"),
      rename = Some("extractedRename"),
      metadata = Some(
        Metadata(
          encoding = Some("extractedEncoding")
        )
      ),
      database = Some("extractedDatabase")
    )

    val sampleCurrentDomain = Domain(
      name = "currentDomain",
      comment = Some("Current Domain Comment"),
      tags = Set("currentTag"),
      rename = Some("currentRename"),
      metadata = Some(
        Metadata(
          encoding = Some("currentEncoding")
        )
      ),
      database = Some("currentDatabase")
    )

    val meldConfig = DomainMelderConfig(
      comment = Extract,
      tags = Extract,
      rename = Extract,
      database = Extract,
      metadata = Extract
    )

    val result = melder.meldDomain(
      meldConfig,
      sampleExtractedDomain,
      Some(sampleCurrentDomain)
    )

    result.comment shouldBe sampleExtractedDomain.comment
    result.tags shouldBe sampleExtractedDomain.tags
    result.rename shouldBe sampleExtractedDomain.rename
    result.database shouldBe sampleExtractedDomain.database
    result.metadata shouldBe sampleExtractedDomain.metadata
  }

  it should "prioritize current values when precedence is set to Current" in {
    val melder = new LoadConfigMelder()
    val sampleExtractedDomain = Domain(
      name = "extractedDomain",
      comment = Some("Extracted Domain Comment"),
      tags = Set("extractTag"),
      rename = Some("extractedRename"),
      metadata = Some(
        Metadata(
          encoding = Some("extractedEncoding")
        )
      ),
      database = Some("extractedDatabase")
    )

    val sampleCurrentDomain = Domain(
      name = "currentDomain",
      comment = Some("Current Domain Comment"),
      tags = Set("currentTag"),
      rename = Some("currentRename"),
      metadata = Some(
        Metadata(
          encoding = Some("currentEncoding")
        )
      ),
      database = Some("currentDatabase")
    )

    val meldConfig = DomainMelderConfig(
      comment = Current,
      tags = Current,
      rename = Current,
      database = Current,
      metadata = Current
    )

    val result = melder.meldDomain(
      meldConfig,
      sampleExtractedDomain,
      Some(sampleCurrentDomain)
    )

    result.comment shouldBe sampleCurrentDomain.comment
    result.tags shouldBe sampleCurrentDomain.tags
    result.rename shouldBe sampleCurrentDomain.rename
    result.database shouldBe sampleCurrentDomain.database
    result.metadata shouldBe sampleCurrentDomain.metadata
  }

  it should "fallback to extracted values if current values are empty" in {
    val melder = new LoadConfigMelder()
    val sampleExtractedDomain = Domain(
      name = "extractedDomain",
      comment = None,
      tags = Set("extractTag"),
      rename = None,
      metadata = Some(
        Metadata(
          encoding = Some("extractedEncoding")
        )
      ),
      database = None
    )

    val sampleCurrentDomain = Domain(
      name = "currentDomain",
      comment = Some("Current Domain Comment"),
      tags = Set("currentTag"),
      rename = Some("currentRename"),
      metadata = Some(
        Metadata(
          encoding = Some("currentEncoding")
        )
      ),
      database = Some("currentDatabase")
    )

    val meldConfig = DomainMelderConfig(
      comment = Extract,
      tags = Extract,
      rename = Extract,
      database = Extract,
      metadata = Extract
    )

    val result = melder.meldDomain(
      meldConfig,
      sampleExtractedDomain,
      Some(sampleCurrentDomain)
    )

    result.comment shouldBe sampleCurrentDomain.comment
    result.tags shouldBe sampleExtractedDomain.tags
    result.rename shouldBe sampleCurrentDomain.rename
    result.metadata shouldBe sampleExtractedDomain.metadata
    result.database shouldBe sampleCurrentDomain.database

    val resultEmptyTest = melder.meldDomain(
      meldConfig,
      sampleExtractedDomain.copy(tags = Set(), metadata = None, comment = Some("")),
      Some(sampleCurrentDomain.copy(database = Some("")))
    )
    resultEmptyTest.comment shouldBe sampleCurrentDomain.comment
    resultEmptyTest.tags shouldBe sampleCurrentDomain.tags
    resultEmptyTest.rename shouldBe sampleCurrentDomain.rename
    resultEmptyTest.metadata shouldBe sampleCurrentDomain.metadata
    resultEmptyTest.database shouldBe None
  }

  "meldTableAttributes" should "merge attributes according to KeepExtract strategy" in {
    val melder = new LoadConfigMelder()

    val extractedAttributes = List(sampleAttr1)
    val currentAttributes = Some(List(currentSampleAttr1, sampleAttr2))

    val result = melder.meldTableAttributes(
      defaultAttributeConfig,
      KeepExtract,
      extractedAttributes,
      currentAttributes
    )

    result.map(_.name) should contain theSameElementsAs extractedAttributes.map(_.name)
  }

  it should "merge attributes according to KeepCurrentScript strategy" in {
    val melder = new LoadConfigMelder()

    val extractedAttributes = List(sampleAttr1)
    val currentAttributes =
      Some(List(currentSampleAttr1, sampleAttr2.copy(script = Some("someScript"))))

    val result = melder.meldTableAttributes(
      defaultAttributeConfig,
      KeepCurrentScript,
      extractedAttributes,
      currentAttributes
    )

    result.map(_.name).toSet shouldBe Set("attr1", "attr2")
    result.find(_.name == "attr2").flatMap(_.script) shouldBe Some("someScript")
  }

  it should "merge attributes according to KeepAll strategy" in {
    val melder = new LoadConfigMelder()

    val extractedAttributes = List(sampleAttr1)
    val currentAttributes = Some(List(currentSampleAttr1, sampleAttr2))

    val result = melder.meldTableAttributes(
      defaultAttributeConfig,
      KeepAll,
      extractedAttributes,
      currentAttributes
    )

    result.map(_.name).toSet shouldBe Set("attr1", "attr2")
  }

  "meldDomainRecursively" should "handle domain and tables merging with attributes" in {
    val melder = new LoadConfigMelder()

    val extractedDomain = Domain(
      name = "TestDomain",
      tables = List(
        Schema(
          "TestTable",
          attributes = List(sampleAttr1),
          pattern = Pattern.compile("\\QTestTable\\E.*")
        )
      )
    )

    val currentDomain = Some(
      Domain(
        name = "TestDomain",
        tables = List(
          Schema(
            "TestTable",
            attributes = List(sampleAttr2),
            pattern = Pattern.compile("\\QTestTable\\E.*")
          )
        )
      )
    )

    val result = melder.meldDomainRecursively(
      defaultDomainMelderConfig,
      defaultTableMelderConfig,
      defaultAttributeConfig,
      KeepAll,
      extractedDomain,
      currentDomain
    )

    val mergedAttributes = result.tables.flatMap(_.attributes)
    mergedAttributes.map(_.name).toSet shouldBe Set("attr1", "attr2")
  }
}
