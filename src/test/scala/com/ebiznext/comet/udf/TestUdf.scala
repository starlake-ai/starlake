package com.ebiznext.comet.udf

import com.ebiznext.comet.config.UdfRegistration
import org.apache.spark.sql.{Row, SparkSession}

class TestUdf extends UdfRegistration {

  val concatWithSpace: (String, String) => String = (first: String, second: String) =>
    first + ' ' + second

  override def register(session: SparkSession): Unit = {
    session.udf.register("concatWithSpace", concatWithSpace)
    session.udf.register("isIgnore", isIgnore)
  }

  val isIgnore = (r: Row) => { r.getAs[String]("value").startsWith("IGNORE") }
}
