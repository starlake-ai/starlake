package ai.starlake.job.atlas

import ai.starlake.TestHelper

class AtlasConfigSpec extends TestHelper {
  new WithSettings {
    "All Atlas Config" should "be known and taken  into account" in {
      val rendered = AtlasConfig.usage()
      println(rendered)
      val expected =
        """
          |Usage: starlake atlas [options]
          |
          |  --delete            Should we delete the previous schemas ?
          |  --folder <value>    Folder with yaml schema files
          |  --uris <value>      Atlas URI
          |  --user <value>      Atlas User
          |  --password <value>  Atlas password
          |  --files <value>     List of YML files
          |""".stripMargin
      rendered.substring(rendered.indexOf("Usage:")).replaceAll("\\s", "") shouldEqual expected
        .replaceAll("\\s", "")

    }
  }
}
