package ai.starlake.integration.utils

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main

class AclDependenciesSpec extends IntegrationTestBase {

  "All ACL Generation" should "succeed" in {
    withEnvs("SL_ROOT" -> theSampleFolder.pathAsString) {
      assert(
        new Main().run(
          Array("acl-dependencies", "--all")
        )
      )
    }
  }

  "Some ACL Generation" should "succeed" in {
    withEnvs("SL_ROOT" -> theSampleFolder.pathAsString) {
      assert(
        new Main().run(
          Array("acl-dependencies", "--grantees", "user:me@me.com,user:you@you.com")
        )
      )
    }
  }

  "JSON ACL Generation" should "succeed" in {
    withEnvs("SL_ROOT" -> theSampleFolder.pathAsString) {
      assert(
        new Main().run(
          Array("acl-dependencies", "--all", "--json")
        )
      )
    }
  }
}
