package ai.starlake.schema.generator

import ai.starlake.TestHelper
import ai.starlake.schema.model.{AutoJobDesc, AutoTaskDesc, WriteMode}
import ai.starlake.utils.YamlSerializer

class YamlSerializerSpec extends TestHelper {
  new WithSettings() {
    "Job toMap" should "should produce the correct map" in {
      val task = AutoTaskDesc(
        "",
        Some("select firstname, lastname, age from {{view}} where age=${age}"),
        "user",
        "user",
        WriteMode.OVERWRITE
      )
      val job =
        AutoJobDesc(
          "user",
          List(task),
          Nil,
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
        "views"    -> Map("user_View" -> "accepted/user"),
        "engine"   -> "SPARK"
      )
      assert((expected.toSet diff jobMap.toSet).toMap.isEmpty)
    }
  }
}
