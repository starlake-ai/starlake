package ai.starlake.utils

import better.files.File
import com.typesafe.scalalogging.LazyLogging

import scala.sys.process.{Process, ProcessLogger}
import scala.util.{Failure, Success, Try}

object GraphVizUtils extends LazyLogging {

  def dot2Svg(outputFile: Option[File], str: String): Unit = {
    dot2Format(outputFile, str, "svg")
  }

  def dot2Png(outputFile: Option[File], str: String): Unit = {
    dot2Format(outputFile, str, "png")
  }

  def save(outputFile: Option[File], result: String): Unit = {
    outputFile match {
      case None => Utils.printOut(result)
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
      val plogger = ProcessLogger(stdout append _, stderr append _)
      val p = Process(s"dot -T$format ${dotFile.pathAsString}  -o ${svgFile.pathAsString}")
      val exitCode = p.run(plogger).exitValue()
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
          Utils.printOut(svgFile.contentAsString)
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
}