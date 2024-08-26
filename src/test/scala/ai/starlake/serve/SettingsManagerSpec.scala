package ai.starlake.serve

import ai.starlake.TestHelper

class SettingsManagerSpec extends TestHelper {

  behavior of "SettingsManagerSpec"

  it should "get updated settings" in {
    new WithSettings() {
      val oldEnv = settings.appConfig.env
      val oldMetadata = settings.appConfig.metadata
      val oldRoot = settings.appConfig.root
      val oldDatabase = settings.appConfig.database
      val (settings2, isNew) =
        MapSettingsManager.getUpdatedSettings(
          "/tmp/my/settings/home",
          Some("test"),
          false
        )
      settings2.appConfig.env shouldBe "test"
      settings2.appConfig.root shouldBe "/tmp/my/settings/home"
      settings2.appConfig.database shouldBe "my-project-id"

      MapSettingsManager.getUpdatedSettings(
        oldRoot,
        Option(oldEnv),
        false
      )

    }
  }
}
