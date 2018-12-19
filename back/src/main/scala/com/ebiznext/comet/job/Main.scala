package com.ebiznext.comet.job

import com.ebiznext.comet.config.DatasetArea
import com.ebiznext.comet.sample.SampleData
import com.ebiznext.comet.schema.handlers.{AirflowLauncher, HdfsStorageHandler, SchemaHandler}
import com.ebiznext.comet.workflow.DatasetWorkflow
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.fs.Path

object Main extends SampleData {
  // uses Jackson YAML to parsing, relies on SnakeYAML for low level handling
  val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())
  mapper.registerModule(DefaultScalaModule)

  val usage =
    """
    Usage: Main ingest domain schema path
    Usage: Main watch
  """

  private def printUsage() = {
    println(
      """
        |Usage :
        |comet business jobname
        |comet watch
        |comet import
        |comet ingest datasetDomain, datasetSchema, datasetPath
      """.stripMargin)
  }

  def main(args: Array[String]) = {
    val storageHandler = new HdfsStorageHandler
    val schemaHandler = new SchemaHandler(storageHandler)

    DatasetArea.init(storageHandler)

    val sh = new HdfsStorageHandler

    DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))

    val validator = new DatasetWorkflow(storageHandler, schemaHandler, new AirflowLauncher)

    if (args.length == 0) println(usage)

    val arglist = args.toList
    println(s"Running DatasetValidator $arglist")
    arglist(0) match {
      case "business" if arglist.length == 2 => validator.businessJob(arglist(1))
      case "import" => validator.loadLanding()
      case "watch" => validator.loadPending()
      case "ingest" if arglist.length == 4 =>
        validator.ingest(arglist(1), arglist(2), arglist(3))
      case _ => printUsage()
    }
  }
}
