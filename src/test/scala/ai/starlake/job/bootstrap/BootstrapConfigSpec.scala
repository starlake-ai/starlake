package ai.starlake.job.bootstrap

import ai.starlake.TestHelper

class BootstrapConfigSpec extends TestHelper {
  new WithSettings() {
    "BootstrapConfig" should
    "parse" in {
      val x = settings
      val expected = """
        |Usage: starlake bootstrap [options]
        |
        |
        |Create a ne project optionally based on a specific template eq. quickstart / userguide
        |
        |  --template <value>  Template to use to bootstrap project
        |""".stripMargin
      val rendered = BootstrapConfig.usage()
      rendered.substring(rendered.indexOf("Usage:")).replaceAll("\\s", "") shouldEqual expected
        .replaceAll("\\s", "")
    }
  }
}
