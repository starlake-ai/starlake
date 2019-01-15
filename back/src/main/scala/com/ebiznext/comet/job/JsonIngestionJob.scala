package com.ebiznext.comet.job

import java.sql.Timestamp
import java.time.Instant

import com.ebiznext.comet.config.{DatasetArea, HiveArea}
import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model._
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.json.JsonUtil
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


class JsonJob(val domain: Domain, val schema: Schema, types: List[Type], path: Path, storageHandler: StorageHandler) extends IngestionJob {
  val metadata: Metadata = domain.metadata.getOrElse(Metadata()).`import`(schema.metadata.getOrElse(Metadata()))

  def loadDataSet(): DataFrame = {
    val df = session.read.format("com.databricks.spark.csv")
      .option("inferSchema", value = false)
      .text(path.toString)
    df.show()
    df
  }

  val schemaSparkType: StructType = schema.sparkType(Types(types))


  def validate(dataset: DataFrame): Unit = {
    val now = Timestamp.from(Instant.now)
    val rdd = dataset.rdd
    dataset.show()
    val checkedRDD = JsonUtil.parseRDD(rdd, schemaSparkType).cache()
    val acceptedRDD: RDD[String] = checkedRDD.filter(_.isRight).map(_.right.get)
    val rejectedRDD: RDD[String] = checkedRDD.filter(_.isLeft).map(_.left.get.mkString("\n"))

    val acceptedDF = session.read.json(acceptedRDD)
    saveRejected(rejectedRDD)
    saveAccepted(acceptedDF)
  }

  def saveAccepted(acceptedDF: DataFrame): Unit = {
    val writeMode = metadata.getWrite()
    val acceptedPath = new Path(DatasetArea.accepted(domain.name), schema.name)
    saveRows(acceptedDF, acceptedPath, writeMode, HiveArea.accepted)
  }

  def saveAccepted(acceptedRDD: RDD[Row]): Unit = {
    val writeMode = metadata.getWrite()
    val acceptedPath = new Path(DatasetArea.accepted(domain.name), schema.name)
    saveRows(session.createDataFrame(acceptedRDD, schemaSparkType), acceptedPath, writeMode, HiveArea.accepted)
  }


  override def name: String = "JsonJob"

  override def run(args: Array[String]): SparkSession = {
    val json =
      """
        |{
        |    "glossary": {
        |        "title": "example glossary",
        |		"GlossDiv": {
        |            "title": "S",
        |			"GlossList": {
        |                "GlossEntry": {
        |                    "ID": "SGML",
        |					"SortAs": "SGML",
        |					"GlossTerm": "Standard Generalized Markup Language",
        |					"Acronym": "SGML",
        |					"Abbrev": "ISO 8879:1986",
        |					"GlossDef": {
        |                        "para": "A meta-markup language, used to create markup languages such as DocBook.",
        |						"GlossSeeAlso": ["GML", "XML"],
        |           "IntArray":[1, 2]
        |                    },
        |					"GlossSee": "markup"
        |                }
        |            }
        |        }
        |    }
        |}
      """.stripMargin

    val res = JsonUtil.parseString(json)
    println(res.toString)
    this.session
  }
}