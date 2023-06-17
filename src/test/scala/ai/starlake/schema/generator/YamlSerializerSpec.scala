package ai.starlake.schema.generator

import ai.starlake.TestHelper
import ai.starlake.schema.model.{AutoJobDesc, AutoTaskDesc, Engine, WriteMode}
import ai.starlake.utils.YamlSerializer

class YamlSerializerSpec extends TestHelper {
  new WithSettings() {
    "Job with no explicit engine toMap" should "should produce the correct map" in {
      val task = AutoTaskDesc(
        name = "",
        sql = Some("select firstname, lastname, age from {{view}} where age=${age}"),
        database = None,
        domain = "user",
        table = "user",
        write = WriteMode.OVERWRITE,
        python = None
      )
      val job =
        AutoJobDesc(
          "user",
          List(task),
          Nil,
          None,
          Some("parquet"),
          Some(false),
          views = Some(Map("user_View" -> "accepted/user"))
        )
      val jobMap = YamlSerializer.toMap(job)
      val expected = Map(
        "name" -> "user",
        "tasks" -> List(
          Map(
            "sql"    -> "select firstname, lastname, age from {{view}} where age=${age}",
            "domain" -> "user",
            "table"  -> "user",
            "write"  -> "OVERWRITE"
          )
        ),
        "format"   -> "parquet",
        "coalesce" -> false,
        "engine"   -> "SPARK",
        "views"    -> Map("user_View" -> "accepted/user")
      )
      assert((jobMap.toSet diff expected.toSet).toMap.isEmpty)
    }
    "Job wit BQ engine toMap" should "should produce the correct map with right engine" in {
      val task = AutoTaskDesc(
        name = "",
        sql = Some("select firstname, lastname, age from dataset.table where age=${age}"),
        None,
        domain = "user",
        table = "user",
        write = WriteMode.OVERWRITE,
        python = None
      )
      val job =
        AutoJobDesc(
          "user",
          List(task),
          Nil,
          None,
          Some("parquet"),
          Some(false),
          engine = Some(Engine.fromString("BQ"))
        )
      val jobMap = YamlSerializer.toMap(job)
      val expected = Map(
        "name" -> "user",
        "tasks" -> List(
          Map(
            "sql"    -> "select firstname, lastname, age from dataset.table where age=${age}",
            "domain" -> "user",
            "table"  -> "user",
            "write"  -> "OVERWRITE"
          )
        ),
        "format"   -> "parquet",
        "coalesce" -> false,
        "engine"   -> "BQ"
      )
      assert((expected.toSet diff jobMap.toSet).toMap.isEmpty)
    }
    "Job with SPARK engine toMap" should "should produce the correct map" in {
      val task = AutoTaskDesc(
        name = "",
        sql = Some("select firstname, lastname, age from {{view}} where age=${age}"),
        database = None,
        domain = "user",
        table = "user",
        write = WriteMode.OVERWRITE,
        python = None
      )
      val job =
        AutoJobDesc(
          "user",
          List(task),
          Nil,
          None,
          Some("parquet"),
          Some(false),
          views = Some(Map("user_View" -> "accepted/user")),
          engine = Some(Engine.fromString("SPARK"))
        )
      val jobMap = YamlSerializer.toMap(job)
      val expected = Map(
        "name" -> "user",
        "tasks" -> List(
          Map(
            "sql"    -> "select firstname, lastname, age from {{view}} where age=${age}",
            "domain" -> "user",
            "table"  -> "user",
            "write"  -> "OVERWRITE"
          )
        ),
        "format"   -> "parquet",
        "coalesce" -> false,
        "views"    -> Map("user_View" -> "accepted/user")
      )
      assert((expected.toSet diff jobMap.toSet).toMap.isEmpty)
    }
  }
}
