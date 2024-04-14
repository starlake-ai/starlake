package ai.starlake.integration

import ai.starlake.job.Main

class ExtractBqSchemaSpec extends IntegrationTestBase {
  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
    "Extract sales_kpi" should "create yaml file in external" in {
      withEnvs("SL_ROOT" -> localDir.pathAsString, "SL_ENV" -> "BQ") {
        assert(
          new Main().run(
            Array(
              "extract-bq-schema",
              "--tables",
              "sales_kpi",
              "--accessToken",
              "ya29.a0Ad52N39EM_0hKc8QaOPas3yk1gVl6OgvJmYht919iBBXx-LpxTa8ZK4YmxozOiTpW6rQAXsokHU4nJ0Iswl3ssbY56wf-8kg1ZShXGabU8xUtn57_Z1zPeALann86QevOkiXUpPOJ9unxrRe74XpK4xo-jfxhsT56bBegWTDswmMaCgYKAQYSARASFQHGX2Mi6S4lisiKGfrziRAH-_nz4w0179"
            )
          )
        )
      }
    }
  }
}
