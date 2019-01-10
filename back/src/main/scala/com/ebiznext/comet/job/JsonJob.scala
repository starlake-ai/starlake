package com.ebiznext.comet.job

import java.sql.Timestamp
import java.time.Instant

import com.ebiznext.comet.config.{DatasetArea, HiveArea}
import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model.{Attribute, Domain, Schema, Type}
import com.ebiznext.comet.utils.Utils
import com.fasterxml.jackson.core.JsonToken._
import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.catalyst.json.JacksonUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.Try


class ComplexJsonJob(domain: Domain, schema: Schema, types: List[Type], path: Path, storageHandler: StorageHandler) extends DsvJob(domain, schema, types, path, storageHandler) {
  override def loadDataSet(): DataFrame = {
    val df = session.read.format("com.databricks.spark.csv")
      .option("inferSchema", value = false)
      .text(path.toString)
    df.show()
    df
  }
}

/**
  * Code here mostly comes from org.apache.spark.sql.execution.datasources.json.InferSchema
  *
  */
object ComplexJsonTask {
  def inferCommonType(t1: DataType, t2: DataType): DataType = {
    TypeCoercion.findTightestCommonTypeOfTwo(t1, t2).getOrElse {
      (t1, t2) match {
        case (DoubleType, _: DecimalType) | (_: DecimalType, DoubleType) =>
          DoubleType

        case (t1: DecimalType, t2: DecimalType) =>
          val scale = math.max(t1.scale, t2.scale)
          val range = math.max(t1.precision - t1.scale, t2.precision - t2.scale)
          if (range + scale > 38) {
            // DecimalType can't support precision > 38
            DoubleType
          } else {
            DecimalType(range + scale, scale)
          }

        case (StructType(fields1), StructType(fields2)) =>
          val newFields = new java.util.ArrayList[StructField]()
          val group1 = fields1.groupBy(_.name).mapValues(_.head)
          val group2 = fields2.groupBy(_.name).mapValues(_.head)

          val group1Names = group1.keys.toList
          val group2Names = group2.keys.toList
          var f1Idx = 0
          var f2Idx = 0

          while (f1Idx < group1Names.length && f2Idx < group2Names.length) {
            val f1Name = group1Names(f1Idx)
            val f2Name = group2Names(f2Idx)
            val comp = f1Name.compareTo(f2Name)
            if (comp == 0) {
              val dataType = inferCommonType(group1(f1Name).dataType, group2(f2Name).dataType)
              newFields.add(StructField(f1Name, dataType, nullable = true))
              f1Idx += 1
              f2Idx += 1
            } else if (comp < 0) { // f1Name < f2Name
              newFields.add(group1(f1Name))
              f1Idx += 1
            } else { // f1Name > f2Name
              newFields.add(group2(f2Name))
              f2Idx += 1
            }
          }
          while (f1Idx < group1Names.length) {
            newFields.add(group1(group1Names(f1Idx)))
            f1Idx += 1
          }
          while (f2Idx < group2Names.length) {
            newFields.add(group2(group2Names(f2Idx)))
            f2Idx += 1
          }
          StructType(newFields.toArray(Array.empty[StructField]))

        case (ArrayType(elementType1, containsNull1), ArrayType(elementType2, containsNull2)) =>
          ArrayType(inferCommonType(elementType1, elementType2), containsNull1 || containsNull2)

        // strings and every string is a Json object.
        case (_, _) => StringType
      }
    }
  }

  def inferSchema(parser: JsonParser): DataType = {
    parser.getCurrentToken match {
      case null | VALUE_NULL => NullType
      case FIELD_NAME =>
        parser.nextToken()
        inferSchema(parser)
      case START_ARRAY =>
        var elementType: DataType = NullType
        while (JacksonUtils.nextUntil(parser, END_ARRAY)) {
          elementType = inferCommonType(elementType, inferSchema(parser))
        }
        ArrayType(elementType)
      case START_OBJECT =>
        val builder = Array.newBuilder[StructField]
        while (JacksonUtils.nextUntil(parser, END_OBJECT)) {
          builder += StructField(parser.getCurrentName, inferSchema(parser), nullable = true)
        }
        val fields: Array[StructField] = builder.result()
        StructType(fields)
      case VALUE_STRING =>
        StringType
      case VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT =>
        import JsonParser.NumberType._
        parser.getNumberType match {
          case INT | LONG =>
            LongType
          case BIG_INTEGER | BIG_DECIMAL =>
            val v = parser.getDecimalValue
            if (Math.max(v.precision(), v.scale()) <= DecimalType.MAX_PRECISION) {
              DecimalType(Math.max(v.precision(), v.scale()), v.scale())
            } else {
              DoubleType
            }
          case FLOAT | DOUBLE =>
            DoubleType
        }
      case VALUE_TRUE | VALUE_FALSE =>
        BooleanType
      case _ =>
        throw new Exception("Should never happen")
    }

  }

  val factory = new JsonFactory()

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

  def validate(session: SparkSession, dataset: DataFrame, attributes: List[Attribute], dateFormat: String, timeFormat: String, types: List[Type], sparkType: StructType): (RDD[Try[DataType]], RDD[Try[DataType]]) = {
    val now = Timestamp.from(Instant.now)
    val rdds = dataset.rdd
    dataset.show()
    val checkedRDD = parseRDD(rdds).cache()
    val acceptedRDD = checkedRDD.filter(_.isSuccess)
    val rejectedRDD = checkedRDD.filter(_.isFailure)
    (rejectedRDD, acceptedRDD)
  }
}