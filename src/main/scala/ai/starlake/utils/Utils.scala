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

package ai.starlake.utils

import ai.starlake.schema.model.{Attribute, WriteMode}
import com.typesafe.scalalogging.Logger

import java.io.{PrintWriter, StringWriter}
import scala.collection.mutable
import scala.reflect.runtime.universe
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object Utils {
  type Closeable = { def close(): Unit }

  /** Handle tansparently autocloseable resources and correctly chain exceptions
    *
    * @param r
    *   : the resource
    * @param f
    *   : the try bloc
    * @tparam T
    *   : resource Type
    * @tparam V
    *   : Try bloc return type
    * @return
    *   : Try bloc return value
    */
  def withResources[T <: Closeable, V](r: => T)(f: T => V): V = {
    val resource: T = r
    require(resource != null, "resource is null")
    var exception: Throwable = null
    try {
      f(resource)
    } catch {
      case e: Throwable =>
        exception = e
        throw e
    } finally {
      closeAndAddSuppressed(exception, resource)
    }
  }

  def loadInstance[T](objectName: String): T = {
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val module = runtimeMirror.staticModule(objectName)
    val obj: universe.ModuleMirror = runtimeMirror.reflectModule(module)
    obj.instance.asInstanceOf[T]
  }

  private def closeAndAddSuppressed(e: Throwable, resource: Closeable): Unit = {
    import scala.language.reflectiveCalls // reflective access of structural type member
    if (e != null) {
      e.printStackTrace()
      try {
        resource.close()
      } catch {
        case NonFatal(suppressed) =>
          e.addSuppressed(suppressed)
      }
    } else {
      resource.close()
    }
  }

  /** If the provided `attempt` is a `Success[T]`, do nothing. If it is a `Failure`, then log the
    * contained exception as a side effect and carry on
    *
    * @param attempt
    * @param logger
    *   the logger onto which to log results
    * @tparam T
    * @return
    *   the original `attempt` with no alteration (everything happens as a side effect)
    */
  def logFailure[T](attempt: Try[T], logger: Logger): Try[T] =
    attempt match {
      case success @ Success(_) =>
        success

      case failure @ Failure(exception) =>
        logException(logger, exception)
        failure
    }

  def logException(logger: Logger, exception: Throwable): Unit = {
    logger.error(exceptionAsString(exception))
  }

  def logIfFailure[T](logger: Logger, res: Try[T]): Try[T] = {
    res match {
      case Failure(e) => logException(logger, e)
      case _          =>
    }
    res
  }

  def exceptionAsString(exception: Throwable): String = {
    val sw = new StringWriter
    exception.printStackTrace(new PrintWriter(sw))
    sw.toString
  }

  def getDBDisposition(writeMode: WriteMode, hasMergeKeyDefined: Boolean): (String, String) = {
    val (createDisposition, writeDisposition) = (hasMergeKeyDefined, writeMode) match {
      case (true, wm) if wm == WriteMode.OVERWRITE || wm == WriteMode.APPEND =>
        ("CREATE_IF_NEEDED", "WRITE_TRUNCATE")
      case (_, WriteMode.OVERWRITE) =>
        ("CREATE_IF_NEEDED", "WRITE_TRUNCATE")
      case (_, WriteMode.APPEND) =>
        ("CREATE_IF_NEEDED", "WRITE_APPEND")
      case (_, WriteMode.ERROR_IF_EXISTS) =>
        ("CREATE_IF_NEEDED", "WRITE_EMPTY")
      case (_, WriteMode.IGNORE) =>
        ("CREATE_NEVER", "WRITE_EMPTY")
      case _ =>
        ("CREATE_IF_NEEDED", "WRITE_TRUNCATE")
    }
    (createDisposition, writeDisposition)
  }

  def subst(
    value: String,
    params: Map[String, String]
  ): String = {
    val paramsList = params.toList
    val paramKeys = paramsList.map { case (name, _) => name }
    val paramValues = paramsList.map { case (_, value) => value }

    val allParams = paramKeys.zip(paramValues)
    allParams.foldLeft(value) { case (acc, (p, v)) =>
      s"\\b($p)\\b".r.replaceAllIn(acc, v)
    }
  }

  def toMap(attributes: List[Attribute]): Map[String, Any] = {
    attributes.map { attribute =>
      attribute.attributes match {
        case Some(attributes) => attribute.name -> toMap(attributes)
        case None             => attribute.name -> attribute
      }
    }.toMap
  }

  /** Utility to extract duplicates and their number of occurrences
    *
    * @param values
    *   : Liste of strings
    * @param errorMessage
    *   : Error Message that should contains placeholders for the value(%s) and number of
    *   occurrences (%d)
    * @return
    *   List of tuples contains for ea ch duplicate the number of occurrences
    */
  def duplicates(values: List[String], errorMessage: String): Either[List[String], Boolean] = {
    val errorList: mutable.MutableList[String] = mutable.MutableList.empty
    val duplicates = values.groupBy(identity).mapValues(_.size).filter { case (_, size) =>
      size > 1
    }
    duplicates.foreach { case (key, size) =>
      errorList += errorMessage.format(key, size)
    }
    if (errorList.nonEmpty)
      Left(errorList.toList)
    else
      Right(true)
  }

  def combine(
    errors1: Either[List[String], Boolean],
    errors2: Either[List[String], Boolean]*
  ): Either[List[String], Boolean] = {
    val allErrors = errors1 :: List(errors2: _*)
    val errors = allErrors.collect { case Left(err) =>
      err
    }.flatten
    if (errors.isEmpty) Right(true) else Left(errors)
  }

  def extractTags(tags: scala.Option[Set[String]]): Set[(String, String)] = {
    tags.getOrElse(Set.empty[String]).map { tag =>
      val hasValue = tag.indexOf('=') > 0
      val keyValuePAir =
        if (hasValue)
          tag.split('=')
        else
          Array(tag, "")
      keyValuePAir(0) -> keyValuePAir(1)
    }
  }
  def isRunningInDatabricks(): Boolean =
    sys.env.contains("DATABRICKS_RUNTIME_VERSION")

  def isRunningTest(): Boolean =
    Thread.currentThread.getStackTrace.exists(_.getClassName.startsWith("org.scalatest."))

  def caseClassToMap(cc: AnyRef): Map[String, Any] = {
    cc.getClass.getDeclaredFields.foldLeft(Map.empty[String, Any]) { (a, f) =>
      f.setAccessible(true)
      f.getType.getName
      a + (f.getName -> f.get(cc))
    }
  }
  def labels(tags: Option[Set[String]]): Map[String, String] =
    tags
      .getOrElse(Set.empty)
      .map { tag =>
        val labelValue = tag.split("=")
        if (labelValue.size == 1)
          (labelValue(0), "")
        else
          (labelValue(0), labelValue(1))
      }
      .toMap

  def escapeMetaCharacters(str: String): String =
    str
      .replaceAll(",", "\\,")
      .replaceAll("\n", "\\\\n")
      .replaceAll("\r", "\\\\r")
      .replaceAll("\t", "\\\\t")
      .replaceAll("\f", "\\\\f")
      .replaceAll("\b", "\\\\b")
      .replaceAll("\u000B", "\\\\v")
      .replaceAll("\u0007", "\\\\a")

}
