package ai.starlake.utils

import ai.starlake.TestHelper
import ai.starlake.job.Cmd
import better.files.File

class CliConfigSpec extends TestHelper {
  new WithSettings() {
    "Generate Documentation" should "succeed" in {
      val commands: List[Cmd[_]] = ai.starlake.job.Main.commands

      val configMap: Map[String, CliConfig[_]] = commands.map { cmd =>
        cmd.command -> cmd
      }.toMap
      val orderedMap = configMap.toList.sortBy { case (command, _) =>
        command
      }.zipWithIndex

      val mdMap = orderedMap.map { case ((command, config), index) =>
        command -> config.markdown(index + 1)
      }
      val mdPath =
        File(
          getClass.getResource("/")
        ) / "../../../../starlake-docs/docs/docs/0800-cli"

      mdPath.createDirectories()
      val pathAsString = mdPath.pathAsString
      mdMap.foreach { case (k, v) =>
        reflect.io.File(s"$pathAsString/$k.md").writeAll(v)
      }
    }
  }
}
