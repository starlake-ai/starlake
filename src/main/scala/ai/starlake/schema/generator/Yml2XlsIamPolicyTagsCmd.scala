package ai.starlake.schema.generator
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.schema.generator.Yml2XlsIamPolicyTags.writeXls
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.{JobResult, YamlSerializer}
import org.apache.hadoop.fs.Path

import scala.util.Try

object Yml2XlsIamPolicyTagsCmd extends Yml2XlsCmd {
  override def run(config: Yml2XlsConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = Try {
    val inputPath = config.iamPolicyTagsFile
      .map(new Path(_)) getOrElse (DatasetArea.iamPolicyTags())

    val iamPolicyTags =
      YamlSerializer.deserializeIamPolicyTags(settings.storageHandler().read(inputPath))
    writeXls(iamPolicyTags, config.xlsDirectory)
  }.map(_ => JobResult.empty)
}
