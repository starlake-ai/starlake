package ai.starlake.job.ingest

import ai.starlake.TestHelper

class IngestJobSpec extends TestHelper {
  new WithSettings() {
    "All Ingest Config" should "be known and taken  into account" in {
      val rendered = IngestCmd.usage()
      val expected =
        """
          |Usage: starlake ingest [options] [domain] [schema] [paths] [options]
          |Generic data ingestion command that loads a single file into a table using an existing schema definition.
          |
          |  domain  Domain name
          |  schema  Schema name
          |  paths   list of comma separated paths
          |  options arguments to be used as substitutions
          |  --scheduledDate <value> Scheduled date for the job, in format yyyy-MM-dd'T'HH:mm:ss.SSSZ
          |  --reportFormat <value>  Report format: console, json, html
          |""".stripMargin
      rendered.substring(rendered.indexOf("Usage:")).replaceAll("\\s", "") shouldEqual expected
        .replaceAll("\\s", "")
    }
  }
}
