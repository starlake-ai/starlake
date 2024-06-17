package ai.starlake.job.convert

import ai.starlake.schema.model.WriteMode
import org.apache.hadoop.fs.Path

case class Parquet2CSVConfig(
  inputFolder: Path = new Path("/"),
  outputFolder: Option[Path] = None,
  domainName: Option[String] = None,
  schemaName: Option[String] = None,
  writeMode: Option[WriteMode] = None,
  deleteSource: Boolean = false,
  options: Map[String, String] = Map.empty,
  partitions: Int = 1
)
