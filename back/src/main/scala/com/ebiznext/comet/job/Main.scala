package com.ebiznext.comet.job

import java.io.InputStream

import com.ebiznext.comet.config.DatasetArea
import com.ebiznext.comet.data.Data
import com.ebiznext.comet.schema.handlers.{AirflowLauncher, HdfsStorageHandler, LaunchHandler, SchemaHandler}
import com.ebiznext.comet.schema.model.SchemaModel
import com.ebiznext.comet.workflow.DatasetWorkflow
import org.apache.hadoop.fs.Path

object Main extends Data {
  val usage = """
    Usage: Main ingest domain schema pat
    Usage: Main watch
  """
  def main(args: Array[String]) = {
    import org.json4s.native.Serialization.{write => jswrite}
    implicit val formats = SchemaModel.formats
    val storageHandler = new HdfsStorageHandler
    val schemaHandler = new SchemaHandler(storageHandler)

    DatasetArea.init(storageHandler)

    val sh = new HdfsStorageHandler
    val domainsPath = new Path(DatasetArea.domains, domain.name + ".json")
    sh.write(jswrite(domain), domainsPath)
    val typesPath = new Path(DatasetArea.types, "types.json")
    sh.write(jswrite(types), typesPath)

    DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))

//    val stream: InputStream = getClass.getResourceAsStream("/SCHEMA-VALID-NOHEADER.dsv")
//    val lines = scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
//    val targetPath = DatasetArea.path(DatasetArea.pending("DOMAIN"), "SCHEMA-VALID-NOHEADER.dsv")
//    storageHandler.write(lines, targetPath)
    
    val validator = new DatasetWorkflow(storageHandler, schemaHandler, new AirflowLauncher)

    if (args.length == 0) println(usage)

    val arglist = args.toList
    println(s"Running DatasetValidator $arglist")
    arglist(0) match {
      case "watch" => validator.loadPending()
      case "ingest" if arglist.length == 4 =>
        validator.ingest(arglist(1), arglist(2), arglist(3))
    }
  }
}
