package ai.starlake.integration.utils

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main

class AclDependenciesSpec extends IntegrationTestBase {

  "All ACL Generation" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString) {
      assert(
        new Main().run(
          Array("acl-dependencies")
        )
      )
    }
  }

  "Some ACL Generation" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString) {
      assert(
        new Main().run(
          Array("acl-dependencies", "--grantees", "user:me@me.com,user:you@you.com")
        )
      )
    }
  }
}
