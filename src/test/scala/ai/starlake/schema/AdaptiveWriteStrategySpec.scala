package ai.starlake.schema
import ai.starlake.TestHelper
import ai.starlake.schema.handlers.FileInfo
import ai.starlake.schema.model._
import org.apache.hadoop.fs.Path
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1}

import java.time.Instant
import java.util.regex.Pattern

class AdaptiveWriteStrategySpec extends TestHelper with TableDrivenPropertyChecks {

  val now: Instant = Instant.now()
  "adapt" should "change schema" in {
    new WithSettings() {
      val givenStrategies: Map[String, String] = Map(
        WriteStrategyType.APPEND.value        -> """group(2) == "APPEND" """,
        WriteStrategyType.OVERWRITE.value     -> """group(2) == "OVERWRITE" """,
        WriteStrategyType.UPSERT_BY_KEY.value -> """group(2) == "UPSERT_BY_KEY" """,
        WriteStrategyType.UPSERT_BY_KEY_AND_TIMESTAMP.value -> """group(2) == "UPSERT_BY_KEY_AND_TIMESTAMP" """,
        WriteStrategyType.OVERWRITE_BY_PARTITION.value -> """group(2) == "OVERWRITE_BY_PARTITION" """
      )
      val givenSchema: Schema = Schema(
        "dummy-schema",
        pattern = Pattern.compile("(.*)\\.(.*)$"),
        Nil,
        Some(
          Metadata(
            sink = Some(
              AllSinks(
                connectionRef = None,
                partition = Some(List("timestampCol"))
              )
            ),
            writeStrategy = Some(
              WriteStrategy(
                types = Some(givenStrategies),
                key = List("key1", "key2"),
                timestamp = Some("timestampCol")
              )
            )
          )
        ),
        None
      )
      val connectionRefs: TableFor1[String] = Table("connectionRef", "bigquery", "spark", "test-pg")
      forAll(connectionRefs) { connectionRef =>
        val testSchema = givenSchema.copy(metadata = givenSchema.metadata.map { m =>
          m.copy(sink = m.sink.map(_.copy(connectionRef = Some(connectionRef))))
        })
        val appendAdaptedSchema = AdaptiveWriteStrategy.adapt(
          testSchema,
          FileInfo(new Path("/tmp/my_file." + WriteStrategyType.APPEND), 10L, now),
          givenStrategies
        )
        appendAdaptedSchema.metadata
          .flatMap(_.writeStrategy)
          .flatMap(_.`type`) shouldBe Some(WriteStrategyType.APPEND)

        val overwriteAdaptedSchema = AdaptiveWriteStrategy.adapt(
          testSchema,
          FileInfo(new Path("/tmp/my_file." + WriteStrategyType.OVERWRITE), 10L, now),
          givenStrategies
        )
        overwriteAdaptedSchema.metadata
          .flatMap(_.writeStrategy)
          .flatMap(_.`type`) shouldBe Some(WriteStrategyType.OVERWRITE)

        val upsertByKeyAdaptedSchema = AdaptiveWriteStrategy.adapt(
          testSchema,
          FileInfo(new Path("/tmp/my_file." + WriteStrategyType.UPSERT_BY_KEY), 10L, now),
          givenStrategies
        )

        upsertByKeyAdaptedSchema.metadata
          .flatMap(_.writeStrategy)
          .flatMap(_.`type`) shouldBe Some(WriteStrategyType.UPSERT_BY_KEY)

        upsertByKeyAdaptedSchema.metadata
          .flatMap(_.writeStrategy)
          .map(_.key) shouldBe Some(List("key1", "key2"))

        val upsertByKeyAndTimestampAdaptedSchema = AdaptiveWriteStrategy.adapt(
          testSchema,
          FileInfo(
            new Path("/tmp/my_file." + WriteStrategyType.UPSERT_BY_KEY_AND_TIMESTAMP),
            10L,
            now
          ),
          givenStrategies
        )

        upsertByKeyAndTimestampAdaptedSchema.metadata
          .flatMap(_.writeStrategy)
          .flatMap(_.`type`) shouldBe Some(WriteStrategyType.UPSERT_BY_KEY_AND_TIMESTAMP)

        upsertByKeyAdaptedSchema.metadata
          .flatMap(_.writeStrategy)
          .map(_.key) shouldBe Some(List("key1", "key2"))

        upsertByKeyAdaptedSchema.metadata
          .flatMap(_.writeStrategy)
          .flatMap(_.timestamp) shouldBe Some("timestampCol")

        val overwriteByPartitionAdaptedSchema = AdaptiveWriteStrategy.adapt(
          testSchema,
          FileInfo(
            new Path("/tmp/my_file." + WriteStrategyType.OVERWRITE_BY_PARTITION),
            10L,
            now
          ),
          givenStrategies
        )
        overwriteByPartitionAdaptedSchema.metadata
          .flatMap(_.writeStrategy)
          .flatMap(_.`type`) shouldBe Some(WriteStrategyType.OVERWRITE_BY_PARTITION)
      }
    }
  }

  "adaptThenGroup" should "change schema and preserve files path" in {
    new WithSettings() {
      val givenStrategies: Map[String, String] = Map(
        WriteStrategyType.APPEND.value        -> """group(2) == "APPEND" """,
        WriteStrategyType.OVERWRITE.value     -> """group(2) == "OVERWRITE" """,
        WriteStrategyType.UPSERT_BY_KEY.value -> """group(2) == "UPSERT_BY_KEY" """,
        WriteStrategyType.UPSERT_BY_KEY_AND_TIMESTAMP.value -> """group(2) == "UPSERT_BY_KEY_AND_TIMESTAMP" """,
        WriteStrategyType.OVERWRITE_BY_PARTITION.value -> """group(2) == "OVERWRITE_BY_PARTITION" """
      )
      val givenSchema: Schema = Schema(
        "dummy-schema",
        pattern = Pattern.compile("(.*)\\.(.*)$"),
        Nil,
        Some(
          Metadata(
            sink = Some(
              AllSinks(
                connectionRef = Some("bigquery"),
                partition = Some(List("timestampCol"))
              )
            ),
            writeStrategy = Some(
              WriteStrategy(
                types = Some(givenStrategies),
                key = List("key1", "key2"),
                timestamp = Some("timestampCol")
              )
            )
          )
        ),
        None
      )
      val complexGroupedSchema: Seq[(Schema, Iterable[FileInfo])] =
        AdaptiveWriteStrategy.adaptThenGroup(
          givenSchema,
          List(
            FileInfo(new Path("/tmp/my_file1." + WriteStrategyType.APPEND), 10L, now),
            FileInfo(new Path("/tmp/my_file2." + WriteStrategyType.APPEND), 10L, now),
            FileInfo(new Path("/tmp/my_file3." + WriteStrategyType.OVERWRITE), 10L, now),
            FileInfo(new Path("/tmp/my_file4." + WriteStrategyType.OVERWRITE), 10L, now),
            FileInfo(new Path("/tmp/my_file5." + WriteStrategyType.APPEND), 10L, now),
            FileInfo(
              new Path("/tmp/my_file6." + WriteStrategyType.UPSERT_BY_KEY),
              10L,
              now
            ),
            FileInfo(
              new Path("/tmp/my_file7." + WriteStrategyType.UPSERT_BY_KEY_AND_TIMESTAMP),
              10L,
              now
            ),
            FileInfo(
              new Path("/tmp/my_file8." + WriteStrategyType.OVERWRITE_BY_PARTITION),
              10L,
              now
            )
          )
        )
      complexGroupedSchema.flatMap { case (schema, fileList) =>
        schema.metadata.flatMap(_.writeStrategy).flatMap(_.`type`.map(_ -> fileList))
      } should be(
        List(
          WriteStrategyType.APPEND -> List(
            FileInfo(new Path("/tmp/my_file1." + WriteStrategyType.APPEND), 10L, now),
            FileInfo(new Path("/tmp/my_file2." + WriteStrategyType.APPEND), 10L, now)
          ),
          WriteStrategyType.OVERWRITE -> List(
            FileInfo(new Path("/tmp/my_file3." + WriteStrategyType.OVERWRITE), 10L, now),
            FileInfo(new Path("/tmp/my_file4." + WriteStrategyType.OVERWRITE), 10L, now)
          ),
          WriteStrategyType.APPEND -> List(
            FileInfo(new Path("/tmp/my_file5." + WriteStrategyType.APPEND), 10L, now)
          ),
          WriteStrategyType.UPSERT_BY_KEY -> List(
            FileInfo(
              new Path("/tmp/my_file6." + WriteStrategyType.UPSERT_BY_KEY),
              10L,
              now
            )
          ),
          WriteStrategyType.UPSERT_BY_KEY_AND_TIMESTAMP -> List(
            FileInfo(
              new Path("/tmp/my_file7." + WriteStrategyType.UPSERT_BY_KEY_AND_TIMESTAMP),
              10L,
              now
            )
          ),
          WriteStrategyType.OVERWRITE_BY_PARTITION -> List(
            FileInfo(
              new Path("/tmp/my_file8." + WriteStrategyType.OVERWRITE_BY_PARTITION),
              10L,
              now
            )
          )
        )
      )

      val simpleGroupedSchema: Seq[(Schema, Iterable[FileInfo])] =
        AdaptiveWriteStrategy.adaptThenGroup(
          givenSchema,
          (1 to 8).map(i =>
            FileInfo(new Path(s"/tmp/my_file$i." + WriteStrategyType.APPEND), 10L, now)
          )
        )
      simpleGroupedSchema.flatMap { case (schema, fileList) =>
        schema.metadata.flatMap(_.writeStrategy).flatMap(_.`type`.map(_ -> fileList))
      } should be(
        List(
          WriteStrategyType.APPEND -> (1 to 8).map(i =>
            FileInfo(new Path(s"/tmp/my_file$i." + WriteStrategyType.APPEND), 10L, now)
          )
        )
      )
    }
  }
}
