package com.ebiznext.comet.schema.generator

import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.config.StorageArea
import com.ebiznext.comet.schema.model.{AutoJobDesc, AutoTaskDesc, WriteMode}

class YamlSerializerSpec extends TestHelper {
  new WithSettings() {
    "Job toMap" should "should produce the correct map" in {
      val task = AutoTaskDesc(
        None,
        Some("select firstname, lastname, age from {{view}} where age=${age}"),
        "user",
        "user",
        WriteMode.OVERWRITE,
        area = Some(StorageArea.fromString("business"))
      )
      val job =
        AutoJobDesc(
          "user",
          List(task),
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
            "sql"     -> "select firstname, lastname, age from {{view}} where age=${age}",
            "domain"  -> "user",
            "dataset" -> "user",
            "write"   -> "OVERWRITE",
            "area"    -> "business"
          )
        ),
        "area"     -> "business",
        "format"   -> "parquet",
        "coalesce" -> false,
        "views"    -> Map("user_View" -> "accepted/user"),
        "engine"   -> "SPARK"
      )
      assert((expected.toSet diff jobMap.toSet).toMap.isEmpty)
    }
  }
}
