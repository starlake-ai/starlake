package ai.starlake.job.sink.es

import ai.starlake.TestHelper

class ESLoadJobSpec extends TestHelper {
  new WithSettings {
    "All ESLoad Config" should "be known and taken  into account" in {
      val rendered = ESLoadConfig.usage()
      val expected =
        """
          |Usage: starlake esload | index [options]
          |
          |  --timestamp <value>      Elasticsearch index timestamp suffix as in {@timestamp|yyyy.MM.dd}
          |  --id <value>             Elasticsearch Document Id
          |  --mapping <value>        Path to Elasticsearch Mapping File
          |  --domain <value>         Domain Name
          |  --schema <value>         Schema Name
          |  --format <value>         Dataset input file : parquet, json or json-array
          |  --dataset <value>        Input dataset path
          |  --conf es.batch.size.entries=1000,es.batch.size.bytes=1mb...
          |   esSpark configuration options.
          |   See https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html
          |""".stripMargin
      rendered.substring(rendered.indexOf("Usage:")).replaceAll("\\s", "") shouldEqual expected
        .replaceAll("\\s", "")

    }
  }
}
