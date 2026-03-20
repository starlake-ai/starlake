package ai.starlake.schema.handlers

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.model._
import ai.starlake.schema.model.Severity._
import ai.starlake.utils.{Utils, YamlSerde}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

import scala.util.{Failure, Success}

object DagHandler extends LazyLogging {

  def listDagNames(storage: StorageHandler)(implicit settings: Settings): List[String] = {
    val dags = storage.list(DatasetArea.dags, ".sl.yml", recursive = false)
    dags.map(_.path.getName().dropRight(".sl.yml".length))
  }

  def checkDagNameValidity(
    dagRef: String,
    storage: StorageHandler
  )(implicit settings: Settings): Either[List[ValidationMessage], Boolean] = {
    val allDagNames = listDagNames(storage)
    val dagName =
      if (dagRef.endsWith(".sl.yml")) dagRef.dropRight(".sl.yml".length) else dagRef
    if (!allDagNames.contains(dagName)) {
      Left(
        List(
          ValidationMessage(
            Error,
            "Table metadata",
            s"dagRef: $dagRef is not a valid DAG reference. Valid DAG references are ${allDagNames.mkString(",")}"
          )
        )
      )
    } else {
      Right(true)
    }
  }

  def deserializedDagGenerationConfigs(
    dagPath: Path,
    storage: StorageHandler
  ): Map[String, DagInfo] = {
    val dagsConfigsPaths =
      storage
        .list(path = dagPath, extension = ".sl.yml", recursive = false)
        .map(_.path)
    dagsConfigsPaths.map { dagsConfigsPath =>
      val dagConfigName = dagsConfigsPath.getName().dropRight(".sl.yml".length)
      val dagFileContent = storage.read(dagsConfigsPath)
      val dagConfig = YamlSerde
        .deserializeYamlDagConfig(
          dagFileContent,
          dagsConfigsPath.toString
        ) match {
        case Success(dagConfig) => dagConfig
        case Failure(err) =>
          logger.error(
            s"Failed to load dag config in $dagsConfigsPath"
          )
          Utils.logException(logger, err)
          throw err
      }
      logger.info(s"Successfully loaded Dag config $dagConfigName in $dagsConfigsPath")
      dagConfigName -> dagConfig
    }.toMap
  }

  def loadDagGenerationConfigs(
    storage: StorageHandler,
    instantiateVars: Boolean
  )(implicit settings: Settings): Map[String, DagInfo] = {
    if (storage.exists(DatasetArea.dags)) {
      val dagMap = deserializedDagGenerationConfigs(DatasetArea.dags, storage)
      dagMap.map { case (dagName, dagInfo) =>
        if (instantiateVars) {
          val scriptDir = Option(System.getenv("SL_SCRIPT_DIR")).filter(_.nonEmpty).getOrElse("")
          val starlakePath = dagInfo.options.get("SL_STARLAKE_PATH")
          if (
            scriptDir.nonEmpty &&
            (starlakePath.contains("starlake") || starlakePath.isEmpty)
          ) {
            dagName -> dagInfo.copy(options =
              dagInfo.options.updated("SL_STARLAKE_PATH", s"$scriptDir/starlake")
            )
          } else
            dagName -> dagInfo
        } else
          dagName -> dagInfo
      }
    } else {
      logger.info("No dags config provided. Use only configuration defined in domain config files.")
      Map.empty[String, DagInfo]
    }
  }
}
