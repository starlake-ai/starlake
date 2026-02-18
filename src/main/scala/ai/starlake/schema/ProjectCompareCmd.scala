package ai.starlake.schema

import ai.starlake.config.Settings
import ai.starlake.job.Cmd
import ai.starlake.schema.handlers.SchemaHandler
import ai.starlake.utils.{JobResult, Utils}
import better.files.File
import scopt.OParser

import scala.util.{Success, Try}

/** Command to compare two Starlake project versions.
  *
  * Usage: starlake compare [options]
  */
object ProjectCompareCmd extends Cmd[ProjectCompareConfig] {
  val command = "compare"

  val parser: OParser[Unit, ProjectCompareConfig] = {
    val builder = OParser.builder[ProjectCompareConfig]
    OParser.sequence(
      builder.programName(s"$shell $command"),
      builder.head(shell, command, "[options]"),
      builder.note(""),
      builder
        .opt[String]("path1")
        .action { (x, c) => c.copy(path1 = x) }
        .optional()
        .text("old version starlake project path"),
      builder
        .opt[String]("path2")
        .action { (x, c) => c.copy(path2 = x) }
        .optional()
        .text("new version starlake project path"),
      builder
        .opt[String]("gitWorkTree")
        .action { (x, c) => c.copy(gitWorkTree = x) }
        .optional()
        .text("local path to git project (only if path1 or path2 if empty)"),
      builder
        .opt[String]("commit1")
        .action { (x, c) => c.copy(commit1 = x) }
        .optional()
        .text("old project commit id (SHA) - if path1 is empty"),
      builder
        .opt[String]("commit2")
        .action { (x, c) => c.copy(commit2 = x) }
        .optional()
        .text("new project commit id (SHA) - if path2 is empty"),
      builder
        .opt[String]("tag1")
        .action { (x, c) => c.copy(tag1 = x) }
        .optional()
        .text("old project git tag (latest for most recent tag) - if path1 and commit1 are empty"),
      builder
        .opt[String]("tag2")
        .action { (x, c) => c.copy(tag2 = x) }
        .optional()
        .text("new project git tag (latest for most recent tag) - if path2 and commit1 are empty"),
      builder
        .opt[String]("template")
        .action { (x, c) => c.copy(template = Some(x)) }
        .optional()
        .text("SSP / Mustache Template path"),
      builder
        .opt[String]("output")
        .action { (x, c) => c.copy(output = Some(x)) }
        .optional()
        .text("Output path"),
      reportFormatOption(builder)((c, x) => c.copy(reportFormat = x))
    )
  }

  private def checkOnlyOneOf(errorMessage: String, args: String*): Unit =
    if (args.count(_.nonEmpty) != 1)
      throw new IllegalArgumentException(errorMessage)

  private def getCommitFromTag(tag: String, gitWorkTree: String): String = {
    val tagName =
      if (tag == "latest") {
        //  git describe --abbrev=0
        val latestTagCommand = Array(
          "git",
          "-C",
          gitWorkTree,
          "describe",
          "--abbrev",
          "0"
        )
        val latestTag = Utils
          .runCommand(latestTagCommand.toIndexedSeq)
          .getOrElse(throw new IllegalArgumentException("latest tag not found!"))
      } else
        tag
    val commitOfTagCommand = Array(
      "git",
      "-C",
      gitWorkTree,
      "rev-list",
      "-n",
      "1",
      s"tags/${tagName}"
    )
    val commitOfTagOutput = Utils.runCommand(commitOfTagCommand.toIndexedSeq)
    commitOfTagOutput match {
      case Success(cmdOutput) =>
        cmdOutput.output
      case _ =>
        throw new IllegalArgumentException("Invalid tag")
    }
  }

  private def getPathFromCommit(commit: String, gitWorkTree: String): String = {
    val path = File.newTemporaryDirectory().pathAsString
    val checkoutCommand = Array(
      "git",
      "-C",
      gitWorkTree,
      "--work-tree",
      path,
      "checkout",
      commit,
      "--",
      "."
    )
    val checkoutOutput = Utils.runCommand(checkoutCommand.toIndexedSeq)
    checkoutOutput match {
      case Success(_) =>
        path
      case _ =>
        throw new IllegalArgumentException("Invalid commit")
    }
  }

  private def getPathFromCommitOrTag(
    gitWorkTree: String,
    path: String,
    commit: String,
    tag: String
  ): String = {
    if (path.isEmpty) {
      val commitFromTag =
        if (commit.isEmpty)
          getCommitFromTag(tag, gitWorkTree)
        else
          commit

      getPathFromCommit(commitFromTag, gitWorkTree)
    } else {
      path
    }
  }

  def parse(args: Seq[String]): Option[ProjectCompareConfig] = {
    val config = OParser.parse(parser, args, ProjectCompareConfig(), setup)
    config.foreach { config =>
      if (config.path1.isEmpty || config.path2.isEmpty) {
        if (config.gitWorkTree.isEmpty) {
          throw new IllegalArgumentException("gitWorkTree is required")
        }
      }

      checkOnlyOneOf(
        "Only one of path1 or commit1 or tag1 is required",
        config.path1,
        config.commit1,
        config.tag1
      )

      checkOnlyOneOf(
        "Only one of path2 or commit2 or tag2 is required",
        config.path2,
        config.commit2,
        config.tag2
      )
    }
    config
  }

  private def cleanTempPath(path: String): Unit = {
    File(path).delete()
  }
  override def run(config: ProjectCompareConfig, schemaHandler: SchemaHandler)(implicit
    settings: Settings
  ): Try[JobResult] = {
    val path1 =
      getPathFromCommitOrTag(config.gitWorkTree, config.path1, config.commit1, config.tag1)

    val path2 =
      getPathFromCommitOrTag(config.gitWorkTree, config.path2, config.commit2, config.tag2)

    val configWithPaths = config.copy(path1 = path1, path2 = path2)

    val result = Try(ProjectCompare.compare(configWithPaths)).map(_ => JobResult.empty)

    if (config.path1.isEmpty) {
      cleanTempPath(path1)
    }

    if (config.path2.isEmpty) {
      cleanTempPath(path2)
    }
    result
  }
}
