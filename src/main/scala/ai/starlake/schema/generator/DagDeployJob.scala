package ai.starlake.schema.generator

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.{EmptyJobResult, JobResult, Utils}
import better.files.File
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.Path

import scala.util.{Success, Try}

class DagDeployJob(schemaHandler: SchemaHandler) extends StrictLogging {
  def deployDags(config: DagDeployConfig)(implicit settings: Settings): Try[JobResult] = Try {
    val inputDir = config.inputDir.getOrElse(new Path(DatasetArea.build, "dags").toString)

    val orchestratorName =
      schemaHandler.orchestratorName().getOrElse(throw new Exception("Orchestrator not configured"))

    logger.info(s"Orchestrator Name: $orchestratorName")

    val outputPath = new Path(config.outputDir)
    if (config.clean)
      settings.storageHandler().delete(outputPath)

    val pythonCmd =
      s"python3 -m pip install --break-system-packages --no-cache-dir --target ${inputDir} starlake-orchestration starlake-$orchestratorName"
    val cmdResult = Utils.runCommand(pythonCmd)
    cmdResult match {
      case Success(result) =>
        logger.info(s"DAG deployment command output: ${result.output}")
        if (result.exit != 0) {
          logger.error(s"DAG deployment command error (if any): ${result.error}")
          throw new Exception(s"DAG deployment command failed: $pythonCmd\n${result.error}")
        } else {
          if (result.error.nonEmpty)
            logger.info(result.error)
        }
        logger.info(s"DAG deployment command succeeded: $pythonCmd")
      case _ =>
        throw new Exception(s"DAG deployment command failed: $pythonCmd")
    }

    val inputFileFolder = File(inputDir)
    inputFileFolder.listRecursively
      .filter(f =>
        !f.isDirectory &&
        !f.pathAsString.contains(".dist-info/") &&
        !f.pathAsString.contains("/__pycache__/")
      ) // Exclude python package metadata
      .foreach { file =>
        val relativePath = {
          val path = file.pathAsString.replace(inputFileFolder.pathAsString, "")
          if (path.startsWith("/"))
            path.substring(1)
          else path
        }
        val targetPath = new Path(outputPath, relativePath)
        settings
          .storageHandler()
          .mkdirs(targetPath.getParent)
        settings
          .storageHandler()
          .copyFromLocal(new Path(file.pathAsString), targetPath)
      }
    EmptyJobResult
  }
}
