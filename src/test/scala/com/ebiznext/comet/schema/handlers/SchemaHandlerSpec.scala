package com.ebiznext.comet.schema.handlers

import java.io.InputStream

import com.ebiznext.comet.TestHelper
import com.ebiznext.comet.config.DatasetArea
import com.ebiznext.comet.workflow.DatasetWorkflow
import org.apache.hadoop.fs.Path
import org.scalatest.{FlatSpec, Matchers}

class SchemaHandlerSpec extends TestHelper {

  //TODO shouldn't we test sth ?
  "Ingest CSV" should "produce file in accepted" in {
    val sh = new HdfsStorageHandler
    val domainsPath = new Path(DatasetArea.domains, "DOMAIN.yml")
    sh.write(loadFile("/sample/DOMAIN.yml"), domainsPath)
    val typesPath = new Path(DatasetArea.types, "types.yml")
    sh.write(loadFile("/sample/types.yml"), typesPath)

    DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))
    val stream: InputStream =
      getClass.getResourceAsStream("/sample/SCHEMA-VALID.dsv")
    val lines =
      scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
    val targetPath =
      DatasetArea.path(DatasetArea.pending("DOMAIN"), "SCHEMA-VALID.dsv")
    storageHandler.write(lines, targetPath)
    val validator =
      new DatasetWorkflow(storageHandler, schemaHandler, new SimpleLauncher)
    validator.loadPending()
  }
  //TODO shouldn't we test sth ?
  "Ingest Dream Contact CSV" should "produce file in accepted" in {
    val sh = new HdfsStorageHandler
    val domainsPath = new Path(DatasetArea.domains, "dream.yml")
    sh.write(loadFile("/sample/dream/dream.yml"), domainsPath)
    val defaultTypesPath = new Path(DatasetArea.types, "default.yml")
    sh.write(loadFile("/sample/default.yml"), defaultTypesPath)
    val typesPath = new Path(DatasetArea.types, "types.yml")
    sh.write(loadFile("/sample/dream/types.yml"), typesPath)
    DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))

    val stream: InputStream =
      getClass.getResourceAsStream("/sample/dream//OneClient_Contact_20190101_090800_008.psv")
    val lines =
      scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
    val targetPath =
      DatasetArea.path(DatasetArea.pending("dream"), "OneClient_Contact_20190101_090800_008.psv")
    storageHandler.write(lines, targetPath)
    val validator =
      new DatasetWorkflow(storageHandler, schemaHandler, new SimpleLauncher)
    validator.loadPending()
  }
  //TODO shouldn't we test sth ?
  "Ingest Dream Segment CSV" should "produce file in accepted" in {
    val sh = new HdfsStorageHandler
    val domainsPath = new Path(DatasetArea.domains, "dream.yml")
    sh.write(loadFile("/sample/dream/dream.yml"), domainsPath)
    val defaultTypesPath = new Path(DatasetArea.types, "default.yml")
    sh.write(loadFile("/sample/default.yml"), defaultTypesPath)
    val typesPath = new Path(DatasetArea.types, "types.yml")
    sh.write(loadFile("/sample/dream/types.yml"), typesPath)
    DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))

    val stream: InputStream =
      getClass.getResourceAsStream("/sample/dream/OneClient_Segmentation_20190101_090800_008.psv")
    val lines =
      scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
    val targetPath =
      DatasetArea.path(
        DatasetArea.pending("dream"),
        "OneClient_Segmentation_20190101_090800_008.psv"
      )
    storageHandler.write(lines, targetPath)
    val validator =
      new DatasetWorkflow(storageHandler, schemaHandler, new SimpleLauncher)
    validator.loadPending()
  }
  //TODO shouldn't we test sth ?
  "Ingest Dream Locations JSON" should "produce file in accepted" in {
    val sh = new HdfsStorageHandler
    val domainsPath = new Path(DatasetArea.domains, "locations.yml")
    sh.write(loadFile("/sample/simple-json-locations/locations.yml"), domainsPath)
    val typesPath = new Path(DatasetArea.types, "types.yml")
    sh.write(loadFile("/sample/simple-json-locations/types.yml"), typesPath)
    DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))

    val stream: InputStream =
      getClass.getResourceAsStream("/sample/simple-json-locations/locations.json")
    val lines =
      scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
    val targetPath =
      DatasetArea.path(DatasetArea.pending("locations"), "locations.json")
    storageHandler.write(lines, targetPath)
    val validator =
      new DatasetWorkflow(storageHandler, schemaHandler, new SimpleLauncher)
    validator.loadPending()
  }
  //TODO TOFIX & shouldn't we test sth ?
//  "Load Business Definition" should "produce business dataset" in {
//    val sh = new HdfsStorageHandler
//    val jobsPath = new Path(DatasetArea.jobs, "sample/metadata/business/business.yml")
//    sh.write(loadFile("/sample/metadata/business/business.yml"), jobsPath)
//    DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))
//    val validator = new DatasetWorkflow(storageHandler, schemaHandler, new SimpleLauncher)
//    validator.autoJob("business1")
//  }
  //TODO shouldn't we test sth ?
  "Load Types" should "deserialize string as pattern" in {
    val typesPath = new Path(DatasetArea.types, "types.yml")
    val sh = new HdfsStorageHandler
    sh.write(loadFile("/sample/types.yml"), typesPath)
    assert(true)
  }
  //TODO TOFIX & shouldn't we test sth ?
//  "Import" should "copy to HDFS" in {
//    val validator = new DatasetWorkflow(storageHandler, schemaHandler, new SimpleLauncher)
//    validator.loadLanding()
//  }
  //TODO shouldn't we test sth ?
  "Watch" should "request Airflow" in {
    val validator =
      new DatasetWorkflow(storageHandler, schemaHandler, new SimpleLauncher)
    validator.loadPending()
  }

}
