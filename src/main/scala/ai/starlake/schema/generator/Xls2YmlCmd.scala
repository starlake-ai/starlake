package ai.starlake.schema.generator

import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.job.Cmd
import ai.starlake.schema.generator.Xls2Yml.{writeDomainsAsYaml, writeIamPolicyTagsAsYaml}
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.schema.model.IamPolicyTags
import ai.starlake.utils.JobResult
import better.files.File
import com.typesafe.scalalogging.StrictLogging
import scopt.OParser

import scala.util.{Success, Try}

trait Xls2YmlCmd extends Cmd[Xls2YmlConfig] with StrictLogging {

  val command = "xls2yml"

  val parser: OParser[Unit, Xls2YmlConfig] = {
    val builder = OParser.builder[Xls2YmlConfig]
    import builder._
    OParser.sequence(
      programName(s"starlake $command"),
      head("starlake", command, "[options]"),
      note(""),
      opt[Seq[String]]("files")
        .action { (x, c) =>
          val allFiles = x.flatMap { f =>
            val file = File(f)
            if (file.isDirectory()) {
              file.collectChildren(_.name.endsWith(".xlsx")).toList
            } else if (file.exists) {
              List(file)
            } else {
              throw new IllegalArgumentException(s"File $file does not exist")
            }
          }

          c.copy(files = allFiles.map(_.pathAsString))
        }
        .required()
        .text("List of Excel files describing domains & schemas or jobs"),
      opt[String]("iamPolicyTagsFile")
        .action((x, c) => c.copy(iamPolicyTagsFile = Some(x)))
        .optional()
        .text("If true generate IAM PolicyTags YML"),
      opt[String]("outputDir")
        .action((x, c) => c.copy(outputPath = Some(x)))
        .optional()
        .text(
          """Path for saving the resulting YAML file(s). Starlake domains path is used by default.""".stripMargin
        ),
      opt[String]("policyFile")
        .action((x, c) => c.copy(policyFile = Some(x)))
        .optional()
        .text(
          """Optional File for centralising ACL & RLS definition.""".stripMargin
        ),
      opt[Boolean]("job")
        .action((x, c) => c.copy(job = x))
        .optional()
        .text("If true generate YML for a Job.")
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class SchemaGenConfig.
    */
  def parse(args: Seq[String]): Option[Xls2YmlConfig] =
    OParser.parse(parser, args, Xls2YmlConfig(), setup)

  override def run(config: Xls2YmlConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    if (config.job) {
      config.files.foreach(
        Xls2YmlAutoJob.generateSchema(_, config.policyFile, config.outputPath)
      )
    } else {
      config.files.foreach { file =>
        logger.info(s"Generating schemas for $file")
        writeDomainsAsYaml(file, config.outputPath)
      }
    }
    config.iamPolicyTagsFile.foreach { iamPolicyTagsPath =>
      val workbook = new XlsIamPolicyTagsReader(InputPath(iamPolicyTagsPath))
      val iamPolicyTags = IamPolicyTags(workbook.iamPolicyTags)
      writeIamPolicyTagsAsYaml(
        iamPolicyTags,
        config.outputPath.getOrElse(DatasetArea.metadata.toString),
        "iam-policy-tags"
      )
    }
    Success(JobResult.empty)
  }
}

object Xls2YmlCmd extends Xls2YmlCmd
