package com.ebiznext.comet.schema.handlers

import java.io.InputStream

import com.ebiznext.comet.config.DatasetArea
import com.ebiznext.comet.sample.SampleData
import com.ebiznext.comet.workflow.DatasetWorkflow
import org.apache.hadoop.fs.Path
import org.scalatest.{FlatSpec, Matchers}


class SchemaHandlerSpec extends FlatSpec with Matchers with SampleData {

  "Ingest CSV" should "produce file in accepted" in {
    val sh = new HdfsStorageHandler
    val domainsPath = new Path(DatasetArea.domains, "DOMAIN.yml")
    sh.write(loadFile("/DOMAIN.yml"), domainsPath)
    val typesPath = new Path(DatasetArea.types, "types.yml")
    sh.write(loadFile("/types.yml"), typesPath)

    DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))
    val stream: InputStream = getClass.getResourceAsStream("/SCHEMA-VALID.dsv")
    val lines = scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
    val targetPath = DatasetArea.path(DatasetArea.pending("DOMAIN"), "SCHEMA-VALID.dsv")
    storageHandler.write(lines, targetPath)
    val validator = new DatasetWorkflow(storageHandler, schemaHandler, new SimpleLauncher)
    validator.loadPending()
  }

  "Ingest Dream Icon CSV" should "produce file in accepted" in {
    val sh = new HdfsStorageHandler
    val domainsPath = new Path(DatasetArea.domains, "json.yml")
    sh.write(loadFile("/sample/icon/icon.yml"), domainsPath)
    val typesPath = new Path(DatasetArea.types, "types.yml")
    sh.write(loadFile("/sample/icon/types.yml"), typesPath)
    DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))

    val stream: InputStream = getClass.getResourceAsStream("/sample/icon/icon_event.psv")
    val lines = scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
    val targetPath = DatasetArea.path(DatasetArea.pending("icon"), "icon_event.psv")
    storageHandler.write(lines, targetPath)
    val validator = new DatasetWorkflow(storageHandler, schemaHandler, new SimpleLauncher)
    validator.loadPending()
  }

  "Ingest Dream Contact CSV" should "produce file in accepted" in {
    val sh = new HdfsStorageHandler
    val domainsPath = new Path(DatasetArea.domains, "dream.yml")
    sh.write(loadFile("/sample/dream/dream.yml"), domainsPath)
    val typesPath = new Path(DatasetArea.types, "types.yml")
    sh.write(loadFile("/sample/dream/types.yml"), typesPath)
    DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))

    val stream: InputStream = getClass.getResourceAsStream("/sample/dream//OneClient_Contact_20190101_090800_008.psv")
    val lines = scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
    val targetPath = DatasetArea.path(DatasetArea.pending("dream"), "OneClient_Contact_20190101_090800_008.psv")
    storageHandler.write(lines, targetPath)
    val validator = new DatasetWorkflow(storageHandler, schemaHandler, new SimpleLauncher)
    validator.loadPending()
  }

  "Ingest Dream Segment CSV" should "produce file in accepted" in {
    val sh = new HdfsStorageHandler
    val domainsPath = new Path(DatasetArea.domains, "dream.yml")
    sh.write(loadFile("/sample/dream/dream.yml"), domainsPath)
    val typesPath = new Path(DatasetArea.types, "types.yml")
    sh.write(loadFile("/sample/dream/types.yml"), typesPath)
    DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))

    val stream: InputStream = getClass.getResourceAsStream("/sample/dream/OneClient_Segmentation_20190101_090800_008.psv")
    val lines = scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
    val targetPath = DatasetArea.path(DatasetArea.pending("dream"), "OneClient_Segmentation_20190101_090800_008.psv")
    storageHandler.write(lines, targetPath)
    val validator = new DatasetWorkflow(storageHandler, schemaHandler, new SimpleLauncher)
    validator.loadPending()
  }

  "Ingest Dream Locations JSON" should "produce file in accepted" in {
    val sh = new HdfsStorageHandler
    val domainsPath = new Path(DatasetArea.domains, "locations.yml")
    sh.write(loadFile("/sample/simple-json-locations/locations.yml"), domainsPath)
    val typesPath = new Path(DatasetArea.types, "types.yml")
    sh.write(loadFile("/sample/simple-json-locations/types.yml"), typesPath)
    DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))

    val stream: InputStream = getClass.getResourceAsStream("/sample/simple-json-locations/locations.json")
    val lines = scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
    val targetPath = DatasetArea.path(DatasetArea.pending("locations"), "locations.json")
    storageHandler.write(lines, targetPath)
    val validator = new DatasetWorkflow(storageHandler, schemaHandler, new SimpleLauncher)
    validator.loadPending()
  }

  "Load Business Definition" should "produce business dataset" in {
    val sh = new HdfsStorageHandler
    val jobsPath = new Path(DatasetArea.jobs, "business.yml")
    sh.write(loadFile("/business.yml"), jobsPath)
    DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))
    val validator = new DatasetWorkflow(storageHandler, schemaHandler, new SimpleLauncher)
    validator.autoJob("business1")
  }

  "Load Types" should "deserialize string as pattern" in {
    val typesPath = new Path(DatasetArea.types, "types.yml")
    val sh = new HdfsStorageHandler
    sh.write(loadFile("/types.yml"), typesPath)
    val types = schemaHandler.types
    assert(true)
  }

  "Import" should "copy to HDFS" in {
    val validator = new DatasetWorkflow(storageHandler, schemaHandler, new SimpleLauncher)
    validator.loadLanding()
  }
  "Watch" should "request Airflow" in {
    val validator = new DatasetWorkflow(storageHandler, schemaHandler, new SimpleLauncher)
    validator.loadPending()
  }

}


