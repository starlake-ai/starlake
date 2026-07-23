package ai.starlake.tests

import ai.starlake.integration.IntegrationTestBase
import better.files.File

class StarlakeTestsFrameworkSpec extends IntegrationTestBase {

  override def theSampleFolder: File =
    starlakeDir / "src" / "test" / "resources" / "test-framework"

  val projectDir: File = File.newTemporaryDirectory("sl-test-framework")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    theSampleFolder.copyTo(projectDir, overwrite = true)
  }

  override protected def cleanup(): Unit = {
    // project lives in a temp dir, nothing to clean between tests
  }

  private def runTransformTest(
    domain: String,
    task: String,
    testName: String
  ): List[StarlakeTestResult] =
    withEnvs("SL_ROOT" -> projectDir.pathAsString) {
      val tests =
        StarlakeTestData.loadTests(load = false, domain, task, testName)(settings)
      StarlakeTestData.runTransforms(tests, StarlakeTestConfig())(settings)._1
    }

  private def runLoadTest(
    domain: String,
    table: String,
    testName: String
  ): List[StarlakeTestResult] =
    withEnvs("SL_ROOT" -> projectDir.pathAsString) {
      val tests =
        StarlakeTestData.loadTests(load = true, domain, table, testName)(settings)
      StarlakeTestData.runLoads(tests, StarlakeTestConfig())(settings)._1
    }

  "a transform unit test" should "execute the transform and succeed when _expected matches" in {
    val results = runTransformTest("kpi", "order_summary", "test_ok")
    results should not be empty
    results.filter(!_.success) shouldBe empty
  }

  it should "fail when _expected does not match the transform output" in {
    val results = runTransformTest("kpi", "order_summary", "test_ko")
    results should not be empty
    results.exists(!_.success) shouldBe true
  }

  it should "apply the SCD2 write strategy against the preloaded target" in {
    val results = runTransformTest("kpi", "customer_history", "scd2")
    results should not be empty
    results.filter(!_.success) shouldBe empty
  }

  "a load unit test" should "load the incoming file and succeed when _expected matches" in {
    val results = runLoadTest("sales", "orders", "test_ok")
    results should not be empty
    results.filter(!_.success) shouldBe empty
  }

  it should "fail when _expected does not match the loaded data" in {
    val results = runLoadTest("sales", "orders", "test_ko")
    results should not be empty
    results.exists(!_.success) shouldBe true
  }

  it should "apply the SCD2 write strategy when loading into an existing table" in {
    val results = runLoadTest("sales", "categories", "scd2")
    results should not be empty
    results.filter(!_.success) shouldBe empty
  }
}
