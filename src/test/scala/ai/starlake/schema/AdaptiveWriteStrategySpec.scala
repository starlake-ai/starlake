package ai.starlake.schema
import ai.starlake.TestHelper
import ai.starlake.schema.handlers.FileInfo
import ai.starlake.schema.model.{
  AllSinks,
  MergeOptions,
  Metadata,
  Partition,
  Schema,
  WriteMode,
  WriteStrategy,
  WriteStrategyType
}
import org.apache.hadoop.fs.Path
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor1}

import java.time.Instant
import java.util.regex.Pattern
import scala.collection.immutable

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
            write = Some(WriteMode.APPEND),
            sink = Some(
              AllSinks(
                connectionRef = None,
                timestamp = Some("timestampCol"),
                partition = Some(
                  Partition(
                    attributes = List("partitionCol1", "partitionCol2")
                  )
                )
              )
            ),
            writeStrategy = Some(WriteStrategy(Some(givenStrategies)))
          )
        ),
        Some(MergeOptions(key = List("key1", "key2"), timestamp = Some("timestampCol"))),
        None
      )
      val connectionRefs: TableFor1[String] = Table("connectionRef", "bigquery", "spark", "test-h2")
      forAll(connectionRefs) { connectionRef =>
        val testSchema = givenSchema.copy(metadata = givenSchema.metadata.map { m =>
          m.copy(sink = m.sink.map(_.copy(connectionRef = Some(connectionRef))))
        })
        val appendAdaptedSchema = AdaptiveWriteStrategy.adapt(
          testSchema,
          FileInfo(new Path("/tmp/my_file." + WriteStrategyType.APPEND), 10L, now),
          givenStrategies
        )
        appendAdaptedSchema.metadata.flatMap(_.write) shouldBe Some(WriteMode.APPEND)
        appendAdaptedSchema.merge shouldBe None
        appendAdaptedSchema.metadata
          .flatMap(_.sink)
          .flatMap(_.dynamicPartitionOverwrite) shouldBe Some(false)
        appendAdaptedSchema.metadata
          .flatMap(_.writeStrategy)
          .flatMap(_.`type`) shouldBe Some(WriteStrategyType.APPEND)

        val overwriteAdaptedSchema = AdaptiveWriteStrategy.adapt(
          testSchema,
          FileInfo(new Path("/tmp/my_file." + WriteStrategyType.OVERWRITE), 10L, now),
          givenStrategies
        )
        overwriteAdaptedSchema.metadata.flatMap(_.write) shouldBe Some(WriteMode.OVERWRITE)
        overwriteAdaptedSchema.merge shouldBe None
        overwriteAdaptedSchema.metadata
          .flatMap(_.sink)
          .flatMap(_.dynamicPartitionOverwrite) shouldBe Some(false)
        overwriteAdaptedSchema.metadata
          .flatMap(_.writeStrategy)
          .flatMap(_.`type`) shouldBe Some(WriteStrategyType.OVERWRITE)

        val upsertByKeyAdaptedSchema = AdaptiveWriteStrategy.adapt(
          testSchema,
          FileInfo(new Path("/tmp/my_file." + WriteStrategyType.UPSERT_BY_KEY), 10L, now),
          givenStrategies
        )
        upsertByKeyAdaptedSchema.metadata.flatMap(_.write) shouldBe Some(WriteMode.APPEND)
        upsertByKeyAdaptedSchema.merge shouldBe Some(MergeOptions(key = List("key1", "key2")))
        upsertByKeyAdaptedSchema.metadata
          .flatMap(_.sink)
          .flatMap(_.dynamicPartitionOverwrite) shouldBe Some(false)
        upsertByKeyAdaptedSchema.metadata
          .flatMap(_.writeStrategy)
          .flatMap(_.`type`) shouldBe Some(WriteStrategyType.UPSERT_BY_KEY)

        val upsertByKeyAndTimestampAdaptedSchema = AdaptiveWriteStrategy.adapt(
          testSchema,
          FileInfo(
            new Path("/tmp/my_file." + WriteStrategyType.UPSERT_BY_KEY_AND_TIMESTAMP),
            10L,
            now
          ),
          givenStrategies
        )
        upsertByKeyAndTimestampAdaptedSchema.metadata.flatMap(_.write) shouldBe Some(
          WriteMode.APPEND
        )
        upsertByKeyAndTimestampAdaptedSchema.merge shouldBe Some(
          MergeOptions(key = List("key1", "key2"), timestamp = Some("timestampCol"))
        )
        upsertByKeyAndTimestampAdaptedSchema.metadata
          .flatMap(_.sink)
          .flatMap(_.dynamicPartitionOverwrite) shouldBe Some(false)
        upsertByKeyAndTimestampAdaptedSchema.metadata
          .flatMap(_.writeStrategy)
          .flatMap(_.`type`) shouldBe Some(WriteStrategyType.UPSERT_BY_KEY_AND_TIMESTAMP)

        val overwriteByPartitionAdaptedSchema = AdaptiveWriteStrategy.adapt(
          testSchema,
          FileInfo(
            new Path("/tmp/my_file." + WriteStrategyType.OVERWRITE_BY_PARTITION),
            10L,
            now
          ),
          givenStrategies
        )
        overwriteByPartitionAdaptedSchema.metadata.flatMap(_.write) shouldBe Some(WriteMode.APPEND)
        overwriteByPartitionAdaptedSchema.merge shouldBe None
        overwriteByPartitionAdaptedSchema.metadata
          .flatMap(_.sink)
          .flatMap(_.dynamicPartitionOverwrite) shouldBe Some(true)
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
            write = Some(WriteMode.APPEND),
            sink = Some(
              AllSinks(
                connectionRef = Some("bigquery"),
                timestamp = Some("timestampCol"),
                partition = Some(
                  Partition(
                    attributes = List("partitionCol1", "partitionCol2")
                  )
                )
              )
            ),
            writeStrategy = Some(WriteStrategy(Some(givenStrategies)))
          )
        ),
        Some(MergeOptions(key = List("key1", "key2"), timestamp = Some("timestampCol"))),
        None
      )
      val complexGroupedSchema: immutable.Seq[(Schema, Iterable[FileInfo])] =
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

      val simpleGroupedSchema: immutable.Seq[(Schema, Iterable[FileInfo])] =
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
