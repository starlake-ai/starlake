package com.ebiznext.comet.job

import com.ebiznext.comet.config.{DatasetArea, Settings}
import com.ebiznext.comet.schema.handlers.{HdfsStorageHandler, SchemaHandler}
import com.ebiznext.comet.workflow.DatasetWorkflow
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.StrictLogging


/**
  * The root of all things.
  *  - importing from landing
  *  - submitting requests to the cron manager
  *  - ingesting the datasets
  *  - running an auto job
  * All these things ared laaunched from here.
  * See printUsage below to understand the CLI syntax.
  *
  */
object Main extends StrictLogging {
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
        |comet job jobname
        |comet watch [+/-DOMAIN1,DOMAIN2,...]
        |comet import
        |comet ingest datasetDomain datasetSchema datasetPath
      """.stripMargin)
  }

  /**
    * @param args depends on the action required
    *             to run a job:
    *   - call "comet job jobname" where jobname is the name of the job
    *             as defined in one of the definition files present in the metadata/jobs folder.
    *             to import files from a local file system
    *   - call "comet import", this will move files in the landing area to the pending area
    *             to watch for files wiating to be processed
    *   - call"comet watch [{+|–}domain1,domain2,domain3]" with a optional domain list separated by a ','.
    *             When called without any domain, will watch for all domain folders in the landing area
    *             When called with a '+' sign, will look only for this domain folders in the landing area
    *             When called with a '-' sign, will look for all domain folder in the landing area except the ones in the command lines.
    *   - call "comet ingest domain schema hdfs://datasets/domain/pending/file.dsv"
    *           to ingest a file defined by its schema in the specified domain
    */
  def main(args: Array[String]) = {
    val storageHandler = new HdfsStorageHandler
    val schemaHandler = new SchemaHandler(storageHandler)

    DatasetArea.init(storageHandler)

    val sh = new HdfsStorageHandler

    DatasetArea.initDomains(storageHandler, schemaHandler.domains.map(_.name))

    val workflow = new DatasetWorkflow(storageHandler, schemaHandler, Settings.comet.getLauncher())

    if (args.length == 0) println(usage)

    val arglist = args.toList
    logger.info(s"Running Comet $arglist")
    arglist.head match {
      case "job" if arglist.length == 2 => workflow.autoJob(arglist(1))
      case "import" => workflow.loadLanding()
      case "watch" =>
        if (arglist.length == 2) {
          val param = arglist(1)
          if (param.startsWith("-"))
            workflow.loadPending(Nil, param.substring(1).split(',').toList)
          else if (param.startsWith("+"))
            workflow.loadPending(param.substring(1).split(',').toList, Nil)
          else
            workflow.loadPending(param.split(',').toList, Nil)
        }
        else
          workflow.loadPending()
      case "ingest" if arglist.length == 4 =>
        workflow.ingest(arglist(1), arglist(2), arglist(3))
      case _ => printUsage()
    }
  }
}
