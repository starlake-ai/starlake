/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package com.ebiznext.comet.schema.model

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.util.regex.Pattern

import com.ebiznext.comet.schema.model.PrimitiveType.{boolean, date, timestamp}
import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.spark.sql.types.StructField

import scala.collection.mutable
import scala.util.Try

/**
  * List of globally defined types
  *
  * @param types : Type list
  */
case class Types(types: List[Type]) {

  def checkValidity(): Either[List[String], Boolean] = {
    val typeNames = types.map(_.name)
    val dup: Either[List[String], Boolean] =
      duplicates(typeNames, s"%s is defined %d times. A type can only be defined once.")
    combine(dup, types.map(_.checkValidity()): _*)
  }
}

/**
  * Semantic Type
  *
  * @param name          : Type name
  * @param pattern       : Pattern use to check that the input data matches the pattern
  * @param primitiveType : Spark Column Type of the attribute
  */
case class Type(
  name: String,
  pattern: String,
  primitiveType: PrimitiveType = PrimitiveType.string,
  zone: Option[String] = None,
  sample: Option[String] = None,
  comment: Option[String] = None,
  indexMapping: Option[IndexMapping] = None
) {

  @JsonIgnore
  def getIndexMapping(): IndexMapping = {
    require(PrimitiveType.primitiveTypes.contains(primitiveType))
    IndexMapping.fromType(primitiveType)
  }

  /**
    * Left :  User defined Pattern
    * Right : Tuple of Pattern for handling the special case of boolean (True Pattern, False Pattern).
    */
  @JsonIgnore
  val compiledPattern: Option[Either[Pattern, (Pattern, Pattern)]] = {
    name match {
      case "string" => None
      case _ =>
        primitiveType match {
          case PrimitiveType.struct => None
          case PrimitiveType.date =>
            None
          case PrimitiveType.timestamp =>
            None
          case PrimitiveType.boolean =>
            val tf = pattern.split("<-TF->")
            Some(
              Right(
                (
                  Pattern
                    .compile(tf(0), Pattern.MULTILINE),
                  Pattern
                    .compile(tf(1), Pattern.MULTILINE)
                )
              )
            )
          case _ =>
            Some(Left(Pattern.compile(pattern, Pattern.MULTILINE)))
        }
    }
  }

  def matches(value: String): Boolean = {
    name match {
      case "string" => true
      case _ =>
        primitiveType match {
          case PrimitiveType.struct => true
          case PrimitiveType.date =>
            Try(date.fromString(value, pattern)).isSuccess
          case PrimitiveType.timestamp =>
            Try(timestamp.fromString(value, pattern, zone.getOrElse(null))).isSuccess
          case PrimitiveType.boolean =>
            // We can get the pattern safely since checkValidity has been called by now
            boolean.matches(value, compiledPattern.get.right.get._1, compiledPattern.get.right.get._2)
          case _ =>
            // We can get the pattern safely since checkValidity has been called by now
            val textPattern = compiledPattern.get.left.get
            textPattern.matcher(value).matches()
        }
    }
  }

  def sparkValue(value: String): Any = {
    primitiveType.fromString(value, pattern, zone.getOrElse(null))
  }

  def sparkType(fieldName: String, nullable: Boolean, comment: Option[String]): StructField = {
    StructField(fieldName, primitiveType.sparkType, nullable)
      .withComment(comment.getOrElse(""))
  }

  def checkValidity(): Either[List[String], Boolean] = {
    val errorList: mutable.MutableList[String] = mutable.MutableList.empty

    val patternIsValid = Try {
      primitiveType match {
        case PrimitiveType.struct =>
        case PrimitiveType.date =>
          new SimpleDateFormat(pattern)
        case PrimitiveType.timestamp =>
          pattern match {
            case "epoch_second" | "epoch_milli"                                  =>
            case _ if PrimitiveType.dateFormatters.keys.toList.contains(pattern) =>
            case _ =>
              DateTimeFormatter.ofPattern(pattern)
          }
        case PrimitiveType.boolean =>
          val tf = pattern.split("<-TF->")
          assert(tf.size == 2)
          Pattern.compile(tf(0), Pattern.MULTILINE)
          Pattern.compile(tf(1), Pattern.MULTILINE)
        case _ =>
          Pattern.compile(pattern)
      }
    }
    if (patternIsValid.isFailure)
      errorList += s"Invalid Pattern $pattern in type $name"
    val ok = sample.forall { sample =>
      this.matches(sample)
    }
    if (!ok)
      errorList += s"Sample $sample does not match pattern $pattern in type $name"
    if (errorList.nonEmpty)
      Left(errorList.toList)
    else
      Right(true)
  }

}
