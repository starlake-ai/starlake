package ai.starlake.integration

import ai.starlake.job.Main

class AclDependenciesSpec extends IntegrationTestBase {

  "All ACL Generation" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString) {
      Main.main(
        Array("acl-dependencies")
      )
    }
  }

  "Some ACL Generation" should "succeed" in {
    withEnvs("SL_ROOT" -> localDir.pathAsString) {
      Main.main(
        Array("acl-dependencies", "--grantees", "user:me@me.com,user:you@you.com")
      )
    }
  }
}
