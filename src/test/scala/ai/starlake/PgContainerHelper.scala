package ai.starlake
import com.dimafeng.testcontainers.{JdbcDatabaseContainer, PostgreSQLContainer}
import org.testcontainers.utility.DockerImageName

trait PgContainerHelper {
  lazy val pgContainer: PostgreSQLContainer = {
    val pgDockerImage = "postgres"
    val pgDockerTag = "latest"
    val pgDockerImageName = DockerImageName.parse(s"$pgDockerImage:$pgDockerTag")
    val initScriptParam =
      JdbcDatabaseContainer.CommonParams(initScriptPath = Option("init-test-pg.sql"))
    val container = PostgreSQLContainer
      .Def(
        pgDockerImageName,
        databaseName = "starlake",
        username = "test",
        password = "test",
        commonJdbcParams = initScriptParam
      )
      .createContainer()
    container.start()
    container
  }
}
