package com.ebiznext.comet.job

import com.ebiznext.comet.config.DatasetArea
import com.ebiznext.comet.data.Data
import com.ebiznext.comet.schema.handlers.{AirflowLauncher, HdfsStorageHandler, SchemaHandler}
import com.ebiznext.comet.workflow.DatasetWorkflow
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.fs.Path

object Main extends Data {
  // uses Jackson YAML to parsing, relies on SnakeYAML for low level handling
  val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
  mapper.registerModule(DefaultScalaModule)

  val usage =
    """
    Usage: Main ingest domain schema path
    Usage: Main watch
  """

  def main(args: Array[String]) = {
    val storageHandler = new HdfsStorageHandler
    val schemaHandler = new SchemaHandler(storageHandler)

    DatasetArea.init(storageHandler)

    val sh = new HdfsStorageHandler
    val domainsPath = new Path(DatasetArea.domains, domain.name + ".yml")
    sh.write(mapper.writeValueAsString(domain), domainsPath)
    val typesPath = new Path(DatasetArea.types, "types.yml")
    sh.write(mapper.writeValueAsString(types), typesPath)

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
