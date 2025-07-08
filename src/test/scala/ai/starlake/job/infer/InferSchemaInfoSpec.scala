package ai.starlake.job.infer

import ai.starlake.TestHelper

class InferSchemaInfoSpec extends TestHelper {
  new WithSettings() {
    "All InferSchema Config" should "be known and taken  into account" in {
      val rendered = InferSchemaCmd.usage()
      val expected =
        """
          |Usage: starlake infer-schema [options]
          |
          |  --domain <value>       Domain Name
          |  --table <value>        Table Name
          |  --input <value>        Dataset Input Path
          |  --outputDir <value>    Domain YAML Output Path
          |  --write <value>        One of Set(OVERWRITE,APPEND)
          |  --format <value>       Force input file format
          |  --rowTag <value>       row tag to use if detected format is XML
          |  --clean                Delete previous YML before writing
          |  --encoding <value>     Input file encoding. Default to UTF-8
          |""".stripMargin
      rendered.substring(rendered.indexOf("Usage:")).replaceAll("\\s", "") shouldEqual expected
        .replaceAll("\\s", "")

    }
  }
}
