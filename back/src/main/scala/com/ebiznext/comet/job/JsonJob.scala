package com.ebiznext.comet.job

import java.sql.Timestamp
import java.time.Instant

import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model.{Attribute, Domain, Schema, Type}
import com.ebiznext.comet.utils.Utils
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.json.JsonTask.{factory, inferSchema}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.Try


class JsonJob(domain: Domain, schema: Schema, types: List[Type], path: Path, storageHandler: StorageHandler) extends DsvJob(domain, schema, types, path, storageHandler) {
  override def loadDataSet(): DataFrame = {
    val df = session.read.format("com.databricks.spark.csv")
      .option("inferSchema", value = false)
      .text(path.toString)
    df.show()
    df
  }

  def parseString(content: String): Try[DataType] = {
    Try {
      Utils.withResources(factory.createParser(content)) { parser =>
        parser.nextToken()
        inferSchema(parser)
      }
    }
  }

  def parseRDD(inputRDD: RDD[Row]): RDD[Try[DataType]] = {
    inputRDD.mapPartitions { partition =>
      partition.map(row => parseString(row.toString()))
    }
  }

  def validate(session: SparkSession,
               dataset: DataFrame,
               attributes: List[Attribute],
               dateFormat: String,
               timeFormat: String,
               types: List[Type],
               sparkType: StructType): (RDD[Try[DataType]], RDD[Try[DataType]]) = {
    val now = Timestamp.from(Instant.now)
    val rdds = dataset.rdd
    dataset.show()
    val checkedRDD = parseRDD(rdds).cache()
    val acceptedRDD = checkedRDD.filter(_.isSuccess)
    val rejectedRDD = checkedRDD.filter(_.isFailure)
    (rejectedRDD, acceptedRDD)
  }
}

