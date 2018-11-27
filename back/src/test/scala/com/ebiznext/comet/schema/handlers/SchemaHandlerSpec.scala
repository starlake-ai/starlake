package com.ebiznext.comet.schema.handlers

import java.io.InputStream

import com.ebiznext.comet.config.DatasetArea
import com.ebiznext.comet.sample.SampleData
import com.ebiznext.comet.workflow.DatasetWorkflow
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.fs.Path
import org.scalatest.{FlatSpec, Matchers}


class SchemaHandlerSpec extends FlatSpec with Matchers with SampleData {
  val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
  // provides all of the Scala goodiness
  mapper.registerModule(DefaultScalaModule)
  val storageHandler = new HdfsStorageHandler
  val schemaHandler = new SchemaHandler(storageHandler)

  DatasetArea.init(storageHandler)

  val sh = new HdfsStorageHandler
  val domainsPath = new Path(DatasetArea.domains, domain.name + ".json")
  sh.write(mapper.writeValueAsString(domain), domainsPath)
  val typesPath = new Path(DatasetArea.types, "types.json")
  sh.write(mapper.writeValueAsString(types), typesPath)

  DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))

  "Ingest CSV" should "produce file in accepted" in {
    val stream: InputStream = getClass.getResourceAsStream("/SCHEMA-VALID-NOHEADER.dsv")
    val lines = scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
    val targetPath = DatasetArea.path(DatasetArea.pending("DOMAIN"), "SCHEMA-VALID-NOHEADER.dsv")
    storageHandler.write(lines, targetPath)
    val validator = new DatasetWorkflow(storageHandler, schemaHandler, new AirflowLauncher)
    validator.loadPending()
  }

}
