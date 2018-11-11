package com.ebiznext.comet.schema.handlers

import java.io.InputStream

import com.ebiznext.comet.config.DatasetArea
import com.ebiznext.comet.data.Data
import com.ebiznext.comet.schema.model.SchemaModel
import com.ebiznext.comet.workflow.DatasetWorkflow
import org.apache.hadoop.fs.Path
import org.scalatest.{FlatSpec, Matchers}

class SchemaHandlerSpec extends FlatSpec with Matchers with Data {
  import org.json4s.native.Serialization.{read => jsread, write => jswrite}
  implicit val formats = SchemaModel.formats
  val storageHandler = new HdfsStorageHandler
  val schemaHandler = new SchemaHandler(storageHandler)
  
  DatasetArea.init(storageHandler)
  
  val sh = new HdfsStorageHandler
  val domainsPath = new Path(DatasetArea.domains, domain.name +".json")
  sh.write(jswrite(domain), domainsPath)
  val typesPath = new Path(DatasetArea.types, "types.json")
  sh.write(jswrite(types), typesPath)

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
