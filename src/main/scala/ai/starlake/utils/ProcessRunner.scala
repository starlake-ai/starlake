package ai.starlake.utils

import better.files.File
import com.typesafe.scalalogging.LazyLogging

import java.io.{ByteArrayOutputStream, OutputStream, PrintWriter}
import scala.sys.process.{Process, ProcessLogger}
import scala.util.Try

object ProcessRunner extends LazyLogging {

  case class CommandOutput(exit: Int, output: String, error: String) {
    override def toString: String = {
      s"""
         |exit: $exit
         |output: $output
         |error: $error
         |""".stripMargin
    }
  }

  def runCommand(cmd: Seq[String], extraEnv: Map[String, String]): Try[CommandOutput] =
    Try {
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

  def runCommand(cmd: Seq[String]): Try[CommandOutput] = runCommand(cmd, Map.empty)

  def runCommand(cmd: String, extraEnv: Map[String, String]): Try[CommandOutput] =
    Try {
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

  def runCommand(cmd: String): Try[CommandOutput] = runCommand(cmd, Map.empty)

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

  def runCommand(
    cmd: String,
    extraEnv: Map[String, String],
    outFile: File,
    errFile: File
  ): Try[CommandOutput] =
    Try {
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
    logger.info(Utils.obfuscate(cmd).mkString(" "))
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

  private def runCommand(
    cmd: String,
    extraEnv: Map[String, String],
    outStream: OutputStream,
    errStream: OutputStream
  ): Int = {
    logger.info(cmd)
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