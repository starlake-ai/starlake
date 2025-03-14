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

import ai.starlake.config.Settings
import ai.starlake.schema.model.Severity._
import ai.starlake.schema.model.{Attribute, ValidationMessage, WriteMode}
import ai.starlake.utils.Formatter._
import better.files.File
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.{JsonSetter, Nulls}
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializerProvider}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.hubspot.jinjava.{Jinjava, JinjavaConfig}
import com.typesafe.scalalogging.{Logger, StrictLogging}
import org.apache.hadoop.fs.Path
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._

import java.io.{ByteArrayOutputStream, OutputStream, PrintWriter, StringWriter}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe
import scala.sys.process.{Process, ProcessLogger}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object Utils extends StrictLogging {
  case class CommandOutput(exit: Int, output: String, error: String)

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
    *   the result of the attempt
    * @param logger
    *   the logger onto which to log results
    * @tparam T
    *   the type of the resulting attempt
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

  def throwFailure[T](attempt: Try[T], logger: Logger): Boolean =
    attempt match {
      case Success(_) =>
        true
      case Failure(exception) =>
        logException(logger, exception)
        throw exception
    }

  def logException(logger: Logger, exception: Throwable): Unit = {
    logger.error(exceptionAsString(exception))
  }

  def exceptionMessagesChainAsString(exception: Throwable): String = {
    val messages = mutable.ListBuffer.empty[String]
    var currentException = exception
    var count = 1

    while (currentException != null) {
      val prefix = ">" * count
      val msg = currentException.getMessage.trim
      val msgPrefix = msg.indexOf("Exception:")
      if (msgPrefix > 0) {
        messages += prefix + " " + msg.substring(msgPrefix + "Exception:".length).trim
      } else {
        messages += prefix + " " + msg.trim
      }
      currentException = currentException.getCause
      count = count + 1
    }
    messages.mkString("\n")
  }

  def exceptionAsString(exception: Throwable): String = {
    val sw = new StringWriter
    exception.printStackTrace(new PrintWriter(sw))
    sw.toString
  }

  def getDBDisposition(
    writeMode: WriteMode
  ): (String, String) = {
    val (createDisposition, writeDisposition) =
      writeMode match {
        case WriteMode.OVERWRITE =>
          ("CREATE_IF_NEEDED", "WRITE_TRUNCATE")
        case WriteMode.APPEND =>
          ("CREATE_IF_NEEDED", "WRITE_APPEND")
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
        case Nil        => attribute.name -> attribute
        case attributes => attribute.name -> toMap(attributes)
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
  def duplicates(
    target: String,
    values: List[String],
    errorMessage: String
  ): Either[List[ValidationMessage], Boolean] = {
    val errorList: mutable.ListBuffer[ValidationMessage] = mutable.ListBuffer.empty
    val duplicates = values.groupBy(identity).mapValues(_.size).filter { case (_, size) =>
      size > 1
    }
    duplicates.foreach { case (key, size) =>
      errorList += ValidationMessage(
        Error,
        target,
        errorMessage.format(key, size)
      )
    }
    if (errorList.nonEmpty)
      Left(errorList.toList)
    else
      Right(true)
  }

  def combine(
    errors1: Either[List[ValidationMessage], Boolean],
    errors2: Either[List[ValidationMessage], Boolean]*
  ): Either[List[ValidationMessage], Boolean] = {
    val allErrors = errors1 :: List(errors2: _*)
    val errors = allErrors.collect { case Left(err) =>
      err
    }.flatten
    if (errors.isEmpty) Right(true) else Left(errors)
  }

  def extractTags(tags: Set[String]): Set[(String, String)] = {
    tags.map { tag =>
      val hasValue = tag.indexOf('=') > 0
      val keyValuePAir =
        if (hasValue)
          tag.split('=')
        else
          Array(tag, "")
      keyValuePAir(0) -> keyValuePAir(1)
    }
  }

  def isDeltaAvailable(): Boolean = {
    try {
      Class.forName("io.delta.tables.DeltaTable")
      true
    } catch {
      case _: ClassNotFoundException => false
    }
  }

  def isRunningInDatabricks(): Boolean =
    sys.env.contains("DATABRICKS_RUNTIME_VERSION")

  def labels(tags: Set[String]): Map[String, String] =
    tags.map { tag =>
      val labelValue = tag.split("=")
      if (labelValue.size == 1)
        (labelValue(0), "")
      else
        (labelValue(0), labelValue(1))
    }.toMap

  def jinjava(implicit settings: Settings): Jinjava = {
    if (_jinjava == null) {
      val curClassLoader = Thread.currentThread.getContextClassLoader
      val res =
        try {
          Thread.currentThread.setContextClassLoader(this.getClass.getClassLoader)
          val config = JinjavaConfig
            .newBuilder()
            .withFailOnUnknownTokens(false)
            .withNestedInterpretationEnabled(false)
            .build()

          new Jinjava(config)
        } finally Thread.currentThread.setContextClassLoader(curClassLoader)
      res.setResourceLocator(new JinjaResourceHandler())
      _jinjava = res
    }
    _jinjava
  }

  private var _jinjava: Jinjava = null

  def resetJinjaClassLoader(): Unit = {
    _jinjava = null
  }

  def parseJinja(str: String, params: Map[String, Any])(implicit
    settings: Settings
  ): String =
    parseJinja(
      List(str),
      params
    ).head

  def parseJinja(str: List[String], params: Map[String, Any])(implicit
    settings: Settings
  ): List[String] = {
    val result = str.map { sql =>
      jinjava
        .render(sql, params.asJava)
        .richFormat(params, Map.empty)
        .trim
    }
    result
  }

  def parseJinjaTpl(
    templateContent: String,
    params: Map[String, Object]
  )(implicit
    settings: Settings
  ): String = {
    parseJinja(templateContent, params)
  }

  def newYamlMapper(): ObjectMapper = {
    val mapper = new ObjectMapper(new YAMLFactory())
    setMapperProperties(mapper)
  }

  def newJsonMapper(): ObjectMapper = {
    val mapper = new ObjectMapper()
    setMapperProperties(mapper)
  }

  def setMapperProperties(mapper: ObjectMapper): ObjectMapper = {
    mapper
      .registerModule(DefaultScalaModule)
      .registerModule(new HadoopModule())
      .registerModule(new StorageLevelModule())
      .setSerializationInclusion(Include.NON_EMPTY)
      .setDefaultSetterInfo(JsonSetter.Value.forValueNulls(Nulls.AS_EMPTY, Nulls.AS_EMPTY))
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper
  }

  def dot2Svg(outputFile: Option[File], str: String): Unit = {
    dot2Format(outputFile, str, "svg")
  }

  def dot2Png(outputFile: Option[File], str: String): Unit = {
    dot2Format(outputFile, str, "png")
  }

  def save(outputFile: Option[File], result: String): Unit = {
    outputFile match {
      case None => println(result)
      case Some(outputFile) =>
        outputFile.parent.createDirectories()
        outputFile.overwrite(result)
    }
  }

  private def dot2Format(outputFile: Option[File], str: String, format: String): Unit = {
    Try {
      val dotFile = File.newTemporaryFile("graph_", ".dot.tmp")
      dotFile.write(str)
      val svgFile =
        outputFile match {
          case Some(outputFile) =>
            outputFile.parent.createDirectories()
            outputFile
          case None =>
            File.newTemporaryFile("graph_", ".svg.tmp")
        }
      val stdout = new StringBuilder
      val stderr = new StringBuilder
      val logger = ProcessLogger(stdout append _, stderr append _)
      val p = Process(s"dot -T$format ${dotFile.pathAsString}  -o ${svgFile.pathAsString}")
      val exitCode = p.run(logger).exitValue()
      if (exitCode != 0) {
        throw new Exception(
          s"""
          ${stdout.toString()}
          ${stderr.toString()}
          Exited with status code $exitCode.
          --> Please make sure that GraphViz is installed and available in your PATH
          """
        )
      }
      dotFile.delete(swallowIOExceptions = false)
      outputFile match {
        case None =>
          println(svgFile.contentAsString)
          svgFile.delete(swallowIOExceptions = false)
        case Some(_) =>
      }
    }
  } match {
    case Success(_) =>
    case Failure(e) =>
      logger.error(
        s"Error while converting dot to $format. Please make sure you installed the GraphViz tool.",
        Utils.exceptionAsString(e)
      )
  }

  def redact(options: Map[String, String]): Map[String, String] = {
    options.map { case (key, value) =>
      if (key.toLowerCase.contains("password")) {
        key -> "********"
      } else {
        key -> value
      }
    }
  }

  def runCommand(cmd: Seq[String], extraEnv: Map[String, String] = Map.empty): Try[CommandOutput] =
    Try {
      logger.info(cmd.mkString(" "))
      val stdoutStream = new ByteArrayOutputStream
      val stderrStream = new ByteArrayOutputStream
      val exitValue = runCommand(cmd, extraEnv, stdoutStream, stderrStream)
      val output = stdoutStream.toString
      val error = stderrStream.toString
      logger.info(output)
      if (exitValue != 0)
        logger.error(error)
      CommandOutput(exitValue, output, error)
    }

  def runCommand(
    cmd: Seq[String],
    extraEnv: Map[String, String],
    outFile: File,
    errFile: File
  ): Try[CommandOutput] =
    Try {
      logger.info(cmd.mkString(" "))
      val stdoutStream = outFile.newOutputStream
      val stderrStream = errFile.newOutputStream

      try {
        val exitValue = runCommand(cmd, extraEnv, stdoutStream, stderrStream)
        CommandOutput(exitValue, outFile.name, errFile.name)
      } catch {
        case e: Exception =>
          logger.error("Error while running command", e)
          CommandOutput(1, "", Utils.exceptionAsString(e))
      } finally {
        stdoutStream.close()
        stderrStream.close()
      }

    }

  private def runCommand(
    cmd: Seq[String],
    extraEnv: Map[String, String],
    outStream: OutputStream,
    errStream: OutputStream
  ): Int = {
    logger.info(cmd.mkString(" "))
    val stdoutWriter = new PrintWriter(outStream)
    val stderrWriter = new PrintWriter(errStream)
    val exitValue =
      Process(cmd, None, extraEnv.toSeq: _*)
        .!(ProcessLogger(stdoutWriter.println, stderrWriter.println))
    stdoutWriter.close()
    stderrWriter.close()
    logger.info("exitValue: " + exitValue)
    exitValue
  }
}

class HadoopModule extends SimpleModule {
  class HadoopPathSerializer(pathClass: Class[Path]) extends StdSerializer[Path](pathClass) {
    def this() = {
      this(classOf[Path])
    }

    override def serialize(value: Path, jGen: JsonGenerator, provider: SerializerProvider): Unit = {
      jGen.writeString(value.toString)
    }
  }
  this.addSerializer(new HadoopPathSerializer())
}

class StorageLevelModule extends SimpleModule {
  class StorageLevelSerializer(storageLevelClass: Class[StorageLevel])
      extends StdSerializer[StorageLevel](storageLevelClass) {
    def this() = {
      this(classOf[StorageLevel])
    }

    override def serialize(
      value: StorageLevel,
      jGen: JsonGenerator,
      provider: SerializerProvider
    ): Unit = {
      def toString(s: StorageLevel): String = s match {
        case NONE                  => "NONE"
        case DISK_ONLY             => "DISK_ONLY"
        case DISK_ONLY_2           => "DISK_ONLY_2"
        case DISK_ONLY_3           => "DISK_ONLY_3"
        case MEMORY_ONLY           => "MEMORY_ONLY"
        case MEMORY_ONLY_2         => "MEMORY_ONLY_2"
        case MEMORY_ONLY_SER       => "MEMORY_ONLY_SER"
        case MEMORY_ONLY_SER_2     => "MEMORY_ONLY_SER_2"
        case MEMORY_AND_DISK       => "MEMORY_AND_DISK"
        case MEMORY_AND_DISK_2     => "MEMORY_AND_DISK_2"
        case MEMORY_AND_DISK_SER   => "MEMORY_AND_DISK_SER"
        case MEMORY_AND_DISK_SER_2 => "MEMORY_AND_DISK_SER_2"
        case OFF_HEAP              => "OFF_HEAP"
        case _ => throw new IllegalArgumentException(s"Invalid StorageLevel: $s")
      }
      jGen.writeString(toString(value))
    }
  }
  this.addSerializer(new StorageLevelSerializer())
}
