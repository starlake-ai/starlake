package com.ebiznext.comet.job.ingest

import com.ebiznext.comet.TestHelper

class IngestJobSpec extends TestHelper {
  new WithSettings() {
    "All Ingest Config" should "be known and taken  into account" in {
      val rendered = IngestConfig.usage()
      val expected =
        """
          |Usage: comet domain schema paths
          |
          |  domain  Domain name
          |  schema  Schema name
          |  paths   list of comma separated paths
          |""".stripMargin
      rendered.substring(rendered.indexOf("Usage:")).replaceAll("\\s", "") shouldEqual expected
        .replaceAll("\\s", "")
    }
  }
}
