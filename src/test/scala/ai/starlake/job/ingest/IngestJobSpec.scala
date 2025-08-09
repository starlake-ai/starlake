package ai.starlake.job.ingest

import ai.starlake.TestHelper

class IngestJobSpec extends TestHelper {
  new WithSettings() {
    "All Ingest Config" should "be known and taken  into account" in {
      val rendered = IngestCmd.usage()
      val expected =
        """
          |Usage: starlake ingest [options] [domain] [schema] [paths] [options]
          |
          |  domain  Domain name
          |  schema  Schema name
          |  paths   list of comma separated paths
          |  options arguments to be used as substitutions
          |  --scheduledDate <value> Scheduled date for the job, in format yyyy-MM-dd'T'HH:mm:ss.SSSZ
          |""".stripMargin
      rendered.substring(rendered.indexOf("Usage:")).replaceAll("\\s", "") shouldEqual expected
        .replaceAll("\\s", "")
    }
  }
}
