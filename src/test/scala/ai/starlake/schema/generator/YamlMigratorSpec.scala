package ai.starlake.schema.generator
import ai.starlake.TestHelper
import ai.starlake.utils.{YamlMigrator, YamlSerde}

class YamlMigratorSpec extends TestHelper {

  "ApplicationMigrator" should "migrate Application to V1" in {
    val expectedConfigStr =
      """
        |version: 1
        |application:
        | env: "env"
        | datasets: "datasets"
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |env: "env"
        |datasets: "datasets"
        |""".stripMargin
    YamlMigrator.V1.ApplicationConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.ApplicationConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate External to V1" in {
    val expectedConfigStr =
      """
        |version: 1
        |external:
        | projects:
        |   - project: "project"
        |     domains: []
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |projects:
        | - project: "project"
        |   domains: []
        |""".stripMargin
    YamlMigrator.V1.ExternalConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.ExternalConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Env to V1" in {
    val expectedConfigStr =
      """
        |version: 1
        |env:
        | - key: "value"
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |- key: "value"
        |""".stripMargin
    YamlMigrator.V1.EnvConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.EnvConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Types to V1" in {
    val expectedConfigStr =
      """
        |version: 1
        |types:
        | - name: "t1"
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |- name: "t1"
        |""".stripMargin
    YamlMigrator.V1.TypesConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TypesConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Refs to V1" in {
    val expectedConfigStr =
      """
        |version: 1
        |refs:
        | - input:
        |     table: ".*"
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |- input:
        |    table: ".*"
        |""".stripMargin
    YamlMigrator.V1.RefsConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.RefsConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Extract to V1" in {
    val expectedConfigStr =
      """
        |version: 1
        |extract:
        |  default:
        |    schema: "s1"
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |globalJdbcSchema:
        |  schema: "s1"
        |""".stripMargin
    YamlMigrator.V1.ExtractConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.ExtractConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Dag to V1" in {
    val expectedConfigStr =
      """
        |version: 1
        |dag:
        | comment: "this is a comment"
        | template: "template file name"
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |comment: "this is a comment"
        |template: "template file name"
        |""".stripMargin
    YamlMigrator.V1.DagConfig.canMigrate(YamlSerde.mapper.readTree(flatConfigStr)) shouldBe true
    YamlMigrator.V1.DagConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  // LOAD MIGRATION
  it should "migrate Load to V1 with dynamic partition overwrite" in {
    val expectedConfigStr =
      """
        |version: 1
        |load:
        | name: "domain name"
        | metadata:
        |   writeStrategy:
        |     type: OVERWRITE_BY_PARTITION
        |   sink:
        |    partition:
        |       - p1
        |       - p2
        | tables:
        |   - metadata:
        |       writeStrategy:
        |         type: OVERWRITE_BY_PARTITION
        |       sink:
        |         partition:
        |           - p3
        |           - p4
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flagConfigStr =
      """
        |name: "domain name"
        |metadata:
        | sink:
        |   type: SNOWFLAKE
        |   partition:
        |     attributes:
        |       - p1
        |       - p2
        |     sampling: 0.5
        |     attribute: p3
        |     options:
        |       - opt1
        |   dynamicPartitionOverwrite: true
        |tables:
        |  - metadata:
        |      sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p3
        |            - p4
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |        dynamicPartitionOverwrite: true
        |""".stripMargin
    YamlMigrator.V1.LoadConfig.canMigrate(
      YamlSerde.mapper.readTree(flagConfigStr)
    ) shouldBe true
    YamlMigrator.V1.LoadConfig.migrate(
      YamlSerde.mapper.readTree(flagConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Load to V1 with sink timestamp precedence" in {
    val expectedConfigStr =
      """
        |version: 1
        |load:
        | name: "domain name"
        | metadata:
        |   writeStrategy:
        |     type: OVERWRITE_BY_PARTITION
        |   sink:
        |    partition:
        |       - time
        | tables:
        |   - metadata:
        |       writeStrategy:
        |         type: OVERWRITE_BY_PARTITION
        |       sink:
        |        partition:
        |           - time2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |name: "domain name"
        |metadata:
        | sink:
        |   type: SNOWFLAKE
        |   partition:
        |     attributes:
        |       - p1
        |       - p2
        |     sampling: 0.5
        |     attribute: p3
        |     options:
        |       - opt1
        |   timestamp: time
        |   dynamicPartitionOverwrite: true
        |tables:
        |  - metadata:
        |      sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |        timestamp: time2
        |        dynamicPartitionOverwrite: true
        |""".stripMargin
    YamlMigrator.V1.LoadConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.LoadConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Load to V1 and ignore write when writeStrategy is already defined" in {
    val expectedConfigStr =
      """
        |version: 1
        |load:
        | name: "domain name"
        | metadata:
        |   writeStrategy:
        |     type: OVERWRITE_BY_PARTITION
        |   sink:
        |    partition:
        |       - p1
        |       - p2
        | tables:
        |   - metadata:
        |       writeStrategy:
        |         type: OVERWRITE_BY_PARTITION
        |       sink:
        |        partition:
        |           - p1
        |           - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |name: "domain name"
        |metadata:
        | write: OVERWRITE
        | sink:
        |   type: SNOWFLAKE
        |   partition:
        |     attributes:
        |       - p1
        |       - p2
        |     sampling: 0.5
        |     attribute: p3
        |     options:
        |       - opt1
        |   dynamicPartitionOverwrite: true
        |tables:
        | - metadata:
        |     write: OVERWRITE
        |     sink:
        |       type: SNOWFLAKE
        |       partition:
        |         attributes:
        |           - p1
        |           - p2
        |         sampling: 0.5
        |         attribute: p3
        |         options:
        |           - opt1
        |       dynamicPartitionOverwrite: true
        |""".stripMargin
    YamlMigrator.V1.LoadConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.LoadConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Load to V1 and consider write when dynamic partition is false or unset" in {
    val expectedConfigStr =
      """
        |version: 1
        |load:
        | name: "domain name"
        | metadata:
        |   writeStrategy:
        |     type: OVERWRITE
        |   sink:
        |    partition:
        |       - p1
        |       - p2
        | tables:
        |   - metadata:
        |       writeStrategy:
        |         type: OVERWRITE
        |       sink:
        |         partition:
        |           - p1
        |           - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |name: "domain name"
        |metadata:
        | write: OVERWRITE
        | sink:
        |   type: SNOWFLAKE
        |   partition:
        |     attributes:
        |       - p1
        |       - p2
        |     sampling: 0.5
        |     attribute: p3
        |     options:
        |       - opt1
        |   dynamicPartitionOverwrite: false
        |tables:
        |  - metadata:
        |      write: OVERWRITE
        |      sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.LoadConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.LoadConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Load to V1 and should not define write strategy" in {
    val expectedConfigStr =
      """
        |version: 1
        |load:
        | name: "domain name"
        | metadata:
        |   sink:
        |    partition:
        |       - p1
        |       - p2
        | tables:
        |   - metadata:
        |       sink:
        |        partition:
        |           - p1
        |           - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |name: "domain name"
        |metadata:
        | sink:
        |   type: SNOWFLAKE
        |   partition:
        |     attributes:
        |       - p1
        |       - p2
        |     sampling: 0.5
        |     attribute: p3
        |     options:
        |       - opt1
        |   dynamicPartitionOverwrite: false
        |tables:
        |  - metadata:
        |      sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.LoadConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.LoadConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Load to V1 and should set UPSERT_BY_KEY_AND_TIMESTAMP on table" in {
    val expectedConfigStr =
      """
        |version: 1
        |load:
        | name: "domain name"
        | metadata:
        |   sink:
        |    partition:
        |       - p1
        |       - p2
        | tables:
        |   - metadata:
        |       writeStrategy:
        |         type: UPSERT_BY_KEY_AND_TIMESTAMP
        |         key:
        |           - k1
        |           - k2
        |         timestamp: t1
        |       sink:
        |        partition:
        |           - p1
        |           - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |name: "domain name"
        |metadata:
        | sink:
        |   type: SNOWFLAKE
        |   partition:
        |     attributes:
        |       - p1
        |       - p2
        |     sampling: 0.5
        |     attribute: p3
        |     options:
        |       - opt1
        |   dynamicPartitionOverwrite: false
        |tables:
        |  - merge:
        |      key:
        |        - k1
        |        - k2
        |      timestamp: t1
        |    metadata:
        |      sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.LoadConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.LoadConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Load to V1 and should set DELETE_THEN_INSERT on table" in {
    val expectedConfigStr =
      """
        |version: 1
        |load:
        | name: "domain name"
        | metadata:
        |   sink:
        |    partition:
        |       - p1
        |       - p2
        | tables:
        |   - metadata:
        |       writeStrategy:
        |         type: DELETE_THEN_INSERT
        |         key:
        |           - k1
        |           - k2
        |       sink:
        |        partition:
        |           - p1
        |           - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |name: "domain name"
        |metadata:
        | sink:
        |   type: SNOWFLAKE
        |   partition:
        |     attributes:
        |       - p1
        |       - p2
        |     sampling: 0.5
        |     attribute: p3
        |     options:
        |       - opt1
        |   dynamicPartitionOverwrite: false
        |tables:
        |  - merge:
        |      key:
        |        - k1
        |        - k2
        |    metadata:
        |      sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.LoadConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.LoadConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Load to V1 and should set write strategy to APPEND on table when merge is defined with only merge timestamp" in {
    val expectedConfigStr =
      """
        |version: 1
        |load:
        | name: "domain name"
        | metadata:
        |   sink:
        |    partition:
        |       - p1
        |       - p2
        | tables:
        |   - metadata:
        |       writeStrategy:
        |         type: APPEND
        |         timestamp: t1
        |       sink:
        |        partition:
        |           - p1
        |           - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |name: "domain name"
        |metadata:
        | sink:
        |   type: SNOWFLAKE
        |   partition:
        |     attributes:
        |       - p1
        |       - p2
        |     sampling: 0.5
        |     attribute: p3
        |     options:
        |       - opt1
        |   dynamicPartitionOverwrite: false
        |tables:
        |  - merge:
        |      timestamp: t1
        |    metadata:
        |      sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.LoadConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.LoadConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Load to V1 and should set write strategy to write value on table when merge is defined with merge timestamp and write" in {
    val expectedConfigStr =
      """
        |version: 1
        |load:
        | name: "domain name"
        | metadata:
        |   sink:
        |    partition:
        |       - p1
        |       - p2
        | tables:
        |   - metadata:
        |       writeStrategy:
        |         type: OVERWRITE
        |         timestamp: t1
        |       sink:
        |        partition:
        |           - p1
        |           - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |name: "domain name"
        |metadata:
        | sink:
        |   type: SNOWFLAKE
        |   partition:
        |     attributes:
        |       - p1
        |       - p2
        |     sampling: 0.5
        |     attribute: p3
        |     options:
        |       - opt1
        |   dynamicPartitionOverwrite: false
        |tables:
        |  - merge:
        |      timestamp: t1
        |    metadata:
        |      write: OVERWRITE
        |      sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.LoadConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.LoadConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Load to V1 and should set write strategy to write mode value on table when no merge and sink" in {
    val expectedConfigStr =
      """
        |version: 1
        |load:
        | name: "domain name"
        | metadata:
        |   sink:
        |    partition:
        |       - p1
        |       - p2
        | tables:
        |   - metadata:
        |       writeStrategy:
        |         type: OVERWRITE
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |name: "domain name"
        |metadata:
        | sink:
        |   type: SNOWFLAKE
        |   partition:
        |     attributes:
        |       - p1
        |       - p2
        |     sampling: 0.5
        |     attribute: p3
        |     options:
        |       - opt1
        |   dynamicPartitionOverwrite: false
        |tables:
        |  - metadata:
        |      write: OVERWRITE
        |""".stripMargin
    YamlMigrator.V1.LoadConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.LoadConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  // TABLES MIGRATION
  it should "migrate Tables to V1 with dynamic partition overwrite" in {
    val expectedConfigStr =
      """
        |version: 1
        |tables:
        |  - metadata:
        |      writeStrategy:
        |        type: OVERWRITE_BY_PARTITION
        |      sink:
        |        partition:
        |          - p3
        |          - p4
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flagConfigStr =
      """
        |schemas:
        |  - metadata:
        |      sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p3
        |            - p4
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |        dynamicPartitionOverwrite: true
        |""".stripMargin
    YamlMigrator.V1.TableConfig.canMigrate(
      YamlSerde.mapper.readTree(flagConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TableConfig.migrate(
      YamlSerde.mapper.readTree(flagConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Tables to V1 with sink timestamp precedence" in {
    val expectedConfigStr =
      """
        |version: 1
        |tables:
        |  - metadata:
        |      writeStrategy:
        |        type: OVERWRITE_BY_PARTITION
        |      sink:
        |       partition:
        |          - time2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |tables:
        |  - metadata:
        |      sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |        timestamp: time2
        |        dynamicPartitionOverwrite: true
        |""".stripMargin
    YamlMigrator.V1.TableConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TableConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Tables to V1 and ignore write when writeStrategy is already defined" in {
    val expectedConfigStr =
      """
        |version: 1
        |tables:
        |  - metadata:
        |      writeStrategy:
        |        type: OVERWRITE_BY_PARTITION
        |      sink:
        |       partition:
        |          - p1
        |          - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |tables:
        | - metadata:
        |     write: OVERWRITE
        |     sink:
        |       type: SNOWFLAKE
        |       partition:
        |         attributes:
        |           - p1
        |           - p2
        |         sampling: 0.5
        |         attribute: p3
        |         options:
        |           - opt1
        |       dynamicPartitionOverwrite: true
        |""".stripMargin
    YamlMigrator.V1.TableConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TableConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Tables to V1 and consider write when dynamic partition is false or unset" in {
    val expectedConfigStr =
      """
        |version: 1
        |tables:
        |  - metadata:
        |      writeStrategy:
        |        type: OVERWRITE
        |      sink:
        |        partition:
        |          - p1
        |          - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |tables:
        |  - metadata:
        |      write: OVERWRITE
        |      sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.TableConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TableConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Tables to V1 and should not define write strategy" in {
    val expectedConfigStr =
      """
        |version: 1
        |tables:
        |  - metadata:
        |      sink:
        |        partition:
        |          - p1
        |          - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |tables:
        |  - metadata:
        |      sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.TableConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TableConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Tables to V1 and should set UPSERT_BY_KEY_AND_TIMESTAMP on table" in {
    val expectedConfigStr =
      """
        |version: 1
        |tables:
        |  - metadata:
        |      writeStrategy:
        |        type: UPSERT_BY_KEY_AND_TIMESTAMP
        |        key:
        |          - k1
        |          - k2
        |        timestamp: t1
        |      sink:
        |        partition:
        |          - p1
        |          - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |tables:
        |  - merge:
        |      key:
        |        - k1
        |        - k2
        |      timestamp: t1
        |    metadata:
        |      sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.TableConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TableConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Tables to V1 and should set DELETE_THEN_INSERT on table" in {
    val expectedConfigStr =
      """
        |version: 1
        |tables:
        |  - metadata:
        |      writeStrategy:
        |        type: DELETE_THEN_INSERT
        |        key:
        |          - k1
        |          - k2
        |      sink:
        |        partition:
        |          - p1
        |          - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |tables:
        |  - merge:
        |      key:
        |        - k1
        |        - k2
        |    metadata:
        |      sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.TableConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TableConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Tables to V1 and should set write strategy to APPEND on table when merge is defined with only merge timestamp" in {
    val expectedConfigStr =
      """
        |version: 1
        |tables:
        |  - metadata:
        |      writeStrategy:
        |        type: APPEND
        |        timestamp: t1
        |      sink:
        |        partition:
        |          - p1
        |          - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |tables:
        |  - merge:
        |      timestamp: t1
        |    metadata:
        |      sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.TableConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TableConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Tables to V1 and should set write strategy to write value on table when merge is defined with merge timestamp and write" in {
    val expectedConfigStr =
      """
        |version: 1
        |tables:
        |  - metadata:
        |      writeStrategy:
        |        type: OVERWRITE
        |        timestamp: t1
        |      sink:
        |        partition:
        |          - p1
        |          - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |tables:
        |  - merge:
        |      timestamp: t1
        |    metadata:
        |      write: OVERWRITE
        |      sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.TableConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TableConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Tables to V1 and should set write strategy to write mode value on table when no merge and sink" in {
    val expectedConfigStr =
      """
        |version: 1
        |tables:
        |  - metadata:
        |      writeStrategy:
        |        type: OVERWRITE
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |tables:
        |  - metadata:
        |      write: OVERWRITE
        |""".stripMargin
    YamlMigrator.V1.TableConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TableConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  // TABLE MIGRATION
  it should "migrate Table to V1 with dynamic partition overwrite" in {
    val expectedConfigStr =
      """
        |version: 1
        |table:
        |  metadata:
        |      writeStrategy:
        |        type: OVERWRITE_BY_PARTITION
        |      sink:
        |        partition:
        |          - p3
        |          - p4
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flagConfigStr =
      """
        |schema:
        |  metadata:
        |      sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p3
        |            - p4
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |        dynamicPartitionOverwrite: true
        |""".stripMargin
    YamlMigrator.V1.TableConfig.canMigrate(
      YamlSerde.mapper.readTree(flagConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TableConfig.migrate(
      YamlSerde.mapper.readTree(flagConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Table to V1 with sink timestamp precedence" in {
    val expectedConfigStr =
      """
        |version: 1
        |table:
        |  metadata:
        |      writeStrategy:
        |        type: OVERWRITE_BY_PARTITION
        |      sink:
        |       partition:
        |          - time2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |table:
        |  metadata:
        |      sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |        timestamp: time2
        |        dynamicPartitionOverwrite: true
        |""".stripMargin
    YamlMigrator.V1.TableConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TableConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Table to V1 and ignore write when writeStrategy is already defined" in {
    val expectedConfigStr =
      """
        |version: 1
        |table:
        |  metadata:
        |      writeStrategy:
        |        type: OVERWRITE_BY_PARTITION
        |      sink:
        |       partition:
        |          - p1
        |          - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |table:
        | metadata:
        |     write: OVERWRITE
        |     sink:
        |       type: SNOWFLAKE
        |       partition:
        |         attributes:
        |           - p1
        |           - p2
        |         sampling: 0.5
        |         attribute: p3
        |         options:
        |           - opt1
        |       dynamicPartitionOverwrite: true
        |""".stripMargin
    YamlMigrator.V1.TableConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TableConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Table to V1 transform user defined writeStrategy types when already defined" in {
    val expectedConfigStr =
      """
        |version: 1
        |table:
        |  metadata:
        |      writeStrategy:
        |        types:
        |          OVERWRITE: FULL_CONDITION
        |          APPEND: DELTA_CONDITION
        |          DELETE_THEN_INSERT: REPLACE_CONDITION
        |        type: DELETE_THEN_INSERT
        |      sink:
        |       partition:
        |          - p1
        |          - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |table:
        | metadata:
        |     write: OVERWRITE
        |     sink:
        |       type: SNOWFLAKE
        |       partition:
        |         attributes:
        |           - p1
        |           - p2
        |         sampling: 0.5
        |         attribute: p3
        |         options:
        |           - opt1
        |       dynamicPartitionOverwrite: true
        |     writeStrategy:
        |       types:
        |        OVERWRITE: FULL_CONDITION
        |        APPEND: DELTA_CONDITION
        |        UPSERT_BY_KEY: REPLACE_CONDITION
        |       type: UPSERT_BY_KEY
        |""".stripMargin
    YamlMigrator.V1.TableConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TableConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Table to V1 and consider write when dynamic partition is false or unset" in {
    val expectedConfigStr =
      """
        |version: 1
        |table:
        |  metadata:
        |      writeStrategy:
        |        type: OVERWRITE
        |      sink:
        |        partition:
        |          - p1
        |          - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |table:
        |  metadata:
        |      write: OVERWRITE
        |      sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.TableConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TableConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Table to V1 and should not define write strategy" in {
    val expectedConfigStr =
      """
        |version: 1
        |table:
        |  metadata:
        |      sink:
        |        partition:
        |          - p1
        |          - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |table:
        |  metadata:
        |      sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.TableConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TableConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Table to V1 and should set UPSERT_BY_KEY_AND_TIMESTAMP on table" in {
    val expectedConfigStr =
      """
        |version: 1
        |table:
        |  metadata:
        |      writeStrategy:
        |        type: UPSERT_BY_KEY_AND_TIMESTAMP
        |        key:
        |          - k1
        |          - k2
        |        timestamp: t1
        |      sink:
        |        partition:
        |          - p1
        |          - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |table:
        |  merge:
        |      key:
        |        - k1
        |        - k2
        |      timestamp: t1
        |  metadata:
        |      sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.TableConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TableConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Table to V1 and should set DELETE_THEN_INSERT on table" in {
    val expectedConfigStr =
      """
        |version: 1
        |table:
        |  metadata:
        |      writeStrategy:
        |        type: DELETE_THEN_INSERT
        |        key:
        |          - k1
        |          - k2
        |      sink:
        |        partition:
        |          - p1
        |          - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |table:
        |  merge:
        |      key:
        |        - k1
        |        - k2
        |  metadata:
        |      sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.TableConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TableConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Table to V1 and should set write strategy to APPEND on table when merge is defined with only merge timestamp" in {
    val expectedConfigStr =
      """
        |version: 1
        |table:
        |  metadata:
        |      writeStrategy:
        |        type: APPEND
        |        timestamp: t1
        |      sink:
        |        partition:
        |          - p1
        |          - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |table:
        |  merge:
        |      timestamp: t1
        |  metadata:
        |      sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.TableConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TableConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Table to V1 and should set write strategy to write value on table when merge is defined with merge timestamp and write" in {
    val expectedConfigStr =
      """
        |version: 1
        |table:
        |  metadata:
        |      writeStrategy:
        |        type: OVERWRITE
        |        timestamp: t1
        |      sink:
        |        partition:
        |          - p1
        |          - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |table:
        |  merge:
        |      timestamp: t1
        |  metadata:
        |      write: OVERWRITE
        |      sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.TableConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TableConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Table to V1 and should set write strategy to write mode value on table when no merge and sink" in {
    val expectedConfigStr =
      """
        |version: 1
        |table:
        |  metadata:
        |      writeStrategy:
        |        type: OVERWRITE
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |table:
        |  metadata:
        |      write: OVERWRITE
        |""".stripMargin
    YamlMigrator.V1.TableConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TableConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  // TRANSFORM MIGRATION
  it should "migrate Transform to V1 with dynamic partition overwrite" in {
    val expectedConfigStr =
      """
        |version: 1
        |transform:
        |  tasks:
        |    - table: "ds"
        |      writeStrategy:
        |          type: OVERWRITE_BY_PARTITION
        |      sink:
        |          partition:
        |            - p3
        |            - p4
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flagConfigStr =
      """
        |tasks:
        |  - dataset: "ds"
        |    engine: spark
        |    sqlEngine: spark2
        |    sink:
        |        partition:
        |          attributes:
        |            - p3
        |            - p4
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |        dynamicPartitionOverwrite: true
        |""".stripMargin
    YamlMigrator.V1.TransformConfig.canMigrate(
      YamlSerde.mapper.readTree(flagConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TransformConfig.migrate(
      YamlSerde.mapper.readTree(flagConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Transform to V1 with sink timestamp precedence" in {
    val expectedConfigStr =
      """
        |version: 1
        |transform:
        |  tasks:
        |    - writeStrategy:
        |          type: OVERWRITE_BY_PARTITION
        |      sink:
        |          partition:
        |            - time2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |tasks:
        |  - sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |        timestamp: time2
        |        dynamicPartitionOverwrite: true
        |""".stripMargin
    YamlMigrator.V1.TransformConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TransformConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Transform to V1 and ignore write when writeStrategy is already defined" in {
    val expectedConfigStr =
      """
        |version: 1
        |transform:
        |  tasks:
        |    - writeStrategy:
        |          type: OVERWRITE_BY_PARTITION
        |      sink:
        |          partition:
        |            - p1
        |            - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |tasks:
        |  - write: OVERWRITE
        |    sink:
        |       type: SNOWFLAKE
        |       partition:
        |         attributes:
        |           - p1
        |           - p2
        |         sampling: 0.5
        |         attribute: p3
        |         options:
        |           - opt1
        |       dynamicPartitionOverwrite: true
        |""".stripMargin
    YamlMigrator.V1.TransformConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TransformConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Transform to V1 and consider write when dynamic partition is false or unset" in {
    val expectedConfigStr =
      """
        |version: 1
        |transform:
        |  tasks:
        |    - writeStrategy:
        |          type: OVERWRITE
        |      sink:
        |          partition:
        |            - p1
        |            - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |tasks:
        |  - write: OVERWRITE
        |    sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.TransformConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TransformConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Transform to V1 and should not define write strategy" in {
    val expectedConfigStr =
      """
        |version: 1
        |transform:
        |  tasks:
        |    - sink:
        |          partition:
        |            - p1
        |            - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |tasks:
        |  - sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.TransformConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TransformConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Transform to V1 and should set UPSERT_BY_KEY_AND_TIMESTAMP on table" in {
    val expectedConfigStr =
      """
        |version: 1
        |transform:
        |  tasks:
        |    - writeStrategy:
        |          type: UPSERT_BY_KEY_AND_TIMESTAMP
        |          key:
        |            - k1
        |            - k2
        |          timestamp: t1
        |      sink:
        |          partition:
        |            - p1
        |            - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |tasks:
        |  - merge:
        |      key:
        |        - k1
        |        - k2
        |      timestamp: t1
        |    sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.TransformConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TransformConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Transform to V1 and should set DELETE_THEN_INSERT on table" in {
    val expectedConfigStr =
      """
        |version: 1
        |transform:
        |  tasks:
        |    - writeStrategy:
        |          type: DELETE_THEN_INSERT
        |          key:
        |            - k1
        |            - k2
        |      sink:
        |          partition:
        |            - p1
        |            - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |tasks:
        |  - merge:
        |      key:
        |        - k1
        |        - k2
        |    sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.TransformConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TransformConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Transform to V1 and should set write strategy to APPEND on table when merge is defined with only merge timestamp" in {
    val expectedConfigStr =
      """
        |version: 1
        |transform:
        |  tasks:
        |    - writeStrategy:
        |          type: APPEND
        |          timestamp: t1
        |      sink:
        |          partition:
        |            - p1
        |            - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |tasks:
        |  - merge:
        |      timestamp: t1
        |    sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.TransformConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TransformConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Transform to V1 and should set write strategy to write value on table when merge is defined with merge timestamp and write" in {
    val expectedConfigStr =
      """
        |version: 1
        |transform:
        |  tasks:
        |    - writeStrategy:
        |          type: OVERWRITE
        |          timestamp: t1
        |      sink:
        |          partition:
        |            - p1
        |            - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |tasks:
        |  - merge:
        |      timestamp: t1
        |    write: OVERWRITE
        |    sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.TransformConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TransformConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Transform to V1 and should set write strategy to write mode value on table when no merge and sink" in {
    val expectedConfigStr =
      """
        |version: 1
        |transform:
        |  tasks:
        |    - writeStrategy:
        |          type: OVERWRITE
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |tasks:
        |  - write: OVERWRITE
        |""".stripMargin
    YamlMigrator.V1.TransformConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TransformConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  // TASK MIGRATION
  it should "migrate Task to V1 with dynamic partition overwrite" in {
    val expectedConfigStr =
      """
        |version: 1
        |task:
        |    table: "ds"
        |    writeStrategy:
        |          type: OVERWRITE_BY_PARTITION
        |    sink:
        |          partition:
        |            - p3
        |            - p4
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flagConfigStr =
      """
        |dataset: "ds"
        |engine: spark
        |sqlEngine: spark2
        |sink:
        |        partition:
        |          attributes:
        |            - p3
        |            - p4
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |        dynamicPartitionOverwrite: true
        |""".stripMargin
    YamlMigrator.V1.TaskConfig.canMigrate(
      YamlSerde.mapper.readTree(flagConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TaskConfig.migrate(
      YamlSerde.mapper.readTree(flagConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Task to V1 with sink timestamp precedence" in {
    val expectedConfigStr =
      """
        |version: 1
        |task:
        |    writeStrategy:
        |          type: OVERWRITE_BY_PARTITION
        |    sink:
        |          partition:
        |            - time2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |        timestamp: time2
        |        dynamicPartitionOverwrite: true
        |""".stripMargin
    YamlMigrator.V1.TaskConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TaskConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Task to V1 and ignore write when writeStrategy is already defined" in {
    val expectedConfigStr =
      """
        |version: 1
        |task:
        |    writeStrategy:
        |          type: OVERWRITE_BY_PARTITION
        |    sink:
        |          partition:
        |            - p1
        |            - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |write: OVERWRITE
        |sink:
        |       type: SNOWFLAKE
        |       partition:
        |         attributes:
        |           - p1
        |           - p2
        |         sampling: 0.5
        |         attribute: p3
        |         options:
        |           - opt1
        |       dynamicPartitionOverwrite: true
        |""".stripMargin
    YamlMigrator.V1.TaskConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TaskConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Task to V1 and consider write when dynamic partition is false or unset" in {
    val expectedConfigStr =
      """
        |version: 1
        |task:
        |    writeStrategy:
        |          type: OVERWRITE
        |    sink:
        |          partition:
        |            - p1
        |            - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |write: OVERWRITE
        |sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.TaskConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TaskConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Task to V1 and should not define write strategy" in {
    val expectedConfigStr =
      """
        |version: 1
        |task:
        |    sink:
        |          partition:
        |            - p1
        |            - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.TaskConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TaskConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Task to V1 and should set UPSERT_BY_KEY_AND_TIMESTAMP on table" in {
    val expectedConfigStr =
      """
        |version: 1
        |task:
        |    writeStrategy:
        |          type: UPSERT_BY_KEY_AND_TIMESTAMP
        |          key:
        |            - k1
        |            - k2
        |          timestamp: t1
        |    sink:
        |          partition:
        |            - p1
        |            - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |merge:
        |      key:
        |        - k1
        |        - k2
        |      timestamp: t1
        |sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.TaskConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TaskConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Task to V1 and should set DELETE_THEN_INSERT on table" in {
    val expectedConfigStr =
      """
        |version: 1
        |task:
        |    writeStrategy:
        |          type: DELETE_THEN_INSERT
        |          key:
        |            - k1
        |            - k2
        |    sink:
        |          partition:
        |            - p1
        |            - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |merge:
        |      key:
        |        - k1
        |        - k2
        |sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.TaskConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TaskConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Task to V1 and should set write strategy to APPEND on table when merge is defined with only merge timestamp" in {
    val expectedConfigStr =
      """
        |version: 1
        |task:
        |  writeStrategy:
        |          type: APPEND
        |          timestamp: t1
        |  sink:
        |          partition:
        |            - p1
        |            - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |merge:
        |      timestamp: t1
        |sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.TaskConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TaskConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Task to V1 and should set write strategy to write value on table when merge is defined with merge timestamp and write" in {
    val expectedConfigStr =
      """
        |version: 1
        |task:
        |    writeStrategy:
        |          type: OVERWRITE
        |          timestamp: t1
        |    sink:
        |          partition:
        |            - p1
        |            - p2
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |merge:
        |  timestamp: t1
        |write: OVERWRITE
        |sink:
        |        type: SNOWFLAKE
        |        partition:
        |          attributes:
        |            - p1
        |            - p2
        |          sampling: 0.5
        |          attribute: p3
        |          options:
        |            - opt1
        |""".stripMargin
    YamlMigrator.V1.TaskConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TaskConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }

  it should "migrate Task to V1 and should set write strategy to write mode value on table when no merge and sink" in {
    val expectedConfigStr =
      """
        |version: 1
        |task:
        |  writeStrategy:
        |    type: OVERWRITE
        |""".stripMargin
    val expectedConfig = YamlSerde.mapper.readTree(expectedConfigStr)
    val flatConfigStr =
      """
        |write: OVERWRITE
        |""".stripMargin
    YamlMigrator.V1.TaskConfig.canMigrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe true
    YamlMigrator.V1.TaskConfig.migrate(
      YamlSerde.mapper.readTree(flatConfigStr)
    ) shouldBe expectedConfig
  }
}
