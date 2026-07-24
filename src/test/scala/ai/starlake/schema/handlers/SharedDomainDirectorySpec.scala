package ai.starlake.schema.handlers

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.schema.model.Severity

class SharedDomainDirectorySpec extends TestHelper {

  "Domains sharing a landing directory with disjoint table patterns" should "load without error and init all dataset areas" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/shared-directory/domain1.sl.yml",
        datasetDomainName = "SHARED1",
        sourceDatasetPathName = ""
      ) {
        cleanMetadata
        deliverSourceDomain("SHARED1", "/sample/shared-directory/domain1.sl.yml")
        deliverSourceTable("SHARED1", "/sample/shared-directory/clients.sl.yml")
        deliverSourceDomain("SHARED2", "/sample/shared-directory/domain2.sl.yml")
        deliverSourceTable("SHARED2", "/sample/shared-directory/transactions.sl.yml")

        val schemaHandler = settings.schemaHandler(reload = true)
        val domains = schemaHandler.domains(reload = true)

        domains.map(_.name).sorted shouldBe List("SHARED1", "SHARED2")
        schemaHandler._domainErrors shouldBe Nil
        // the two domains point at the same landing directory
        domains.flatMap(_.resolveDirectoryOpt()).distinct.size shouldBe 1
        // domains are cached and dataset areas are initialized for every domain
        schemaHandler._domains.map(_.map(_.name).sorted) shouldBe Some(List("SHARED1", "SHARED2"))
        List("SHARED1", "SHARED2").foreach { domain =>
          storageHandler.exists(DatasetArea.stage(domain)) shouldBe true
          storageHandler.exists(DatasetArea.unresolved(domain)) shouldBe true
          storageHandler.exists(DatasetArea.archive(domain)) shouldBe true
          storageHandler.exists(DatasetArea.replay(domain)) shouldBe true
        }
      }
    }
  }

  "Domains sharing a landing directory with overlapping table patterns" should "be rejected with an error" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/shared-directory/overlap1.sl.yml",
        datasetDomainName = "OVERLAP1",
        sourceDatasetPathName = ""
      ) {
        cleanMetadata
        deliverSourceDomain("OVERLAP1", "/sample/shared-directory/overlap1.sl.yml")
        deliverSourceTable("OVERLAP1", "/sample/shared-directory/clients.sl.yml")
        deliverSourceDomain("OVERLAP2", "/sample/shared-directory/overlap2.sl.yml")
        deliverSourceTable("OVERLAP2", "/sample/shared-directory/transactions_overlap.sl.yml")

        val schemaHandler = settings.schemaHandler(reload = true)
        schemaHandler.domains(reload = true)

        val errors = schemaHandler._domainErrors.filter(_.target == "Domain directory")
        errors.size shouldBe 1
        errors.head.severity shouldBe Severity.Error
        errors.head.message should include("OVERLAP1")
        errors.head.message should include("OVERLAP2")
        errors.head.message should include("clients-.*")
        // domains are not cached and dataset areas are not initialized
        schemaHandler._domains shouldBe None
        List("OVERLAP1", "OVERLAP2").foreach { domain =>
          storageHandler.exists(DatasetArea.stage(domain)) shouldBe false
        }
      }
    }
  }

  "A table pattern declared twice within one domain sharing a directory" should "be rejected with an error" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/shared-directory/overlap1.sl.yml",
        datasetDomainName = "OVERLAP1",
        sourceDatasetPathName = ""
      ) {
        cleanMetadata
        deliverSourceDomain("OVERLAP1", "/sample/shared-directory/overlap1.sl.yml")
        deliverSourceTable("OVERLAP1", "/sample/shared-directory/clients.sl.yml")
        deliverSourceTable("OVERLAP1", "/sample/shared-directory/clients_bis.sl.yml")
        deliverSourceDomain("OVERLAP2", "/sample/shared-directory/overlap2.sl.yml")
        deliverSourceTable("OVERLAP2", "/sample/shared-directory/transactions.sl.yml")

        val schemaHandler = settings.schemaHandler(reload = true)
        schemaHandler.domains(reload = true)

        val errors = schemaHandler._domainErrors.filter(_.target == "Domain directory")
        errors.size shouldBe 1
        errors.head.severity shouldBe Severity.Error
        errors.head.message should include("clients-.*")
        errors.head.message should include("2 tables")
        schemaHandler._domains shouldBe None
      }
    }
  }

  "Duplicated domain names sharing a directory" should "not cache domains nor init dataset areas" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/shared-directory/domain1.sl.yml",
        datasetDomainName = "SHARED1",
        sourceDatasetPathName = ""
      ) {
        cleanMetadata
        // same domain config (name SHARED1) delivered under two folders, disjoint table patterns
        deliverSourceDomain("SHARED1", "/sample/shared-directory/domain1.sl.yml")
        deliverSourceTable("SHARED1", "/sample/shared-directory/clients.sl.yml")
        deliverSourceDomain("SHARED1BIS", "/sample/shared-directory/domain1.sl.yml")
        deliverSourceTable("SHARED1BIS", "/sample/shared-directory/transactions.sl.yml")

        val schemaHandler = settings.schemaHandler(reload = true)
        schemaHandler.domains(reload = true)

        schemaHandler._domainErrors.filter(_.target == "Domain name").size shouldBe 1
        schemaHandler._domainErrors.filter(_.target == "Domain directory") shouldBe Nil
        // name duplication keeps the fail-fast behavior: nothing cached, no area initialized
        schemaHandler._domains shouldBe None
      }
    }
  }

  "A single domain per directory" should "keep loading without error" in {
    new WithSettings() {
      new SpecTrait(
        sourceDomainOrJobPathname = "/sample/shared-directory/domain1.sl.yml",
        datasetDomainName = "SHARED1",
        sourceDatasetPathName = ""
      ) {
        cleanMetadata
        deliverSourceDomain("SHARED1", "/sample/shared-directory/domain1.sl.yml")
        deliverSourceTable("SHARED1", "/sample/shared-directory/clients.sl.yml")

        val schemaHandler = settings.schemaHandler(reload = true)
        val domains = schemaHandler.domains(reload = true)

        domains.map(_.name) shouldBe List("SHARED1")
        schemaHandler._domainErrors shouldBe Nil
        storageHandler.exists(DatasetArea.stage("SHARED1")) shouldBe true
      }
    }
  }
}
