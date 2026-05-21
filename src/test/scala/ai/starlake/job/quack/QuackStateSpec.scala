package ai.starlake.job.quack

import ai.starlake.TestHelper

class QuackStateSpec extends TestHelper {
  new WithSettings() {

    "QuackState" should "round-trip through JSON" in {
      val st = QuackState(
        connection = "warehouse",
        pid        = 12345L,
        bind       = "127.0.0.1",
        port       = 9494,
        logFile    = "/tmp/warehouse.log",
        startedAt  = 1716297600000L
      )
      val json    = QuackState.toJson(st)
      val decoded = QuackState.fromJson(json)
      decoded shouldBe st
    }

    "QuackState.stateDir" should "be $SL_ROOT/.quack" in {
      QuackState.stateDir(settings).pathAsString should endWith("/.quack")
    }

    "QuackState.stateFile" should "live under stateDir keyed by connection name" in {
      QuackState.stateFile("warehouse")(settings).name shouldBe "warehouse.json"
    }

    "QuackState.list" should "skip files whose pid is dead and remove them" in {
      val dir = QuackState.stateDir(settings).createDirectoryIfNotExists(createParents = true)
      val stale = QuackState(
        connection = "ghost",
        pid        = Int.MaxValue.toLong, // very unlikely to be alive
        bind       = "127.0.0.1",
        port       = 9999,
        logFile    = "/tmp/ghost.log",
        startedAt  = 0L
      )
      val stalePath = dir / "ghost.json"
      stalePath.overwrite(QuackState.toJson(stale))

      val live = QuackState.list()(settings)
      live.exists(_.connection == "ghost") shouldBe false
      stalePath.exists shouldBe false
    }
  }
}