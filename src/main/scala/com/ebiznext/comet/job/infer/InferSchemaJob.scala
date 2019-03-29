package com.ebiznext.comet.job.infer

import java.util.regex.Pattern

import com.ebiznext.comet.schema.handlers.InferSchemaHandler
import com.ebiznext.comet.schema.model.Attribute
import com.ebiznext.comet.utils.SparkJob
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object InferSchemaJob extends SparkJob{

  /** Read file without specifying the format
    *
    * @param path : file path
    * @return a dataset of string that contains data file
    */
  def readFile(path: Path) = {
    session.read
      .textFile(path.toString)
  }


  //TODO manage mode
  /** Get domain name
    *
    * @return the domain name
    */
  def getMode: String = "FILE"

  /** Get extension file
    *
    * @param path : file path
    * @return the file extension
    */
  def getExtensionFile(path: Path) ={
    val fileName = path.getName

    if(fileName.contains("."))
      fileName.split("\\.").last
    else
      ""
  }

  /** Get format file
    *
    * @param datasetInit : created dataset without specifying format
    * @param extension : extension file
    * @return
    */
  def getFormatFile(datasetInit: Dataset[String], extension : String): String = {
    val firstLine = datasetInit.first()

    firstLine.charAt(1).toString match {
      case "{"  => "JSON"
      case "[" => "ARRAY_JSON"
      case _  => if (extension.matches(".*sv$")) "DSV"
            else throw new Exception("The format of this file is not supported")
    }
  }

  /** Get separator file
    *
    * @param datasetInit : created dataset without specifying format
    * @return the file separator
    */
  def getSeparator(datasetInit: Dataset[String]) = {
    session.sparkContext
      .parallelize(datasetInit.take(10))
      .map(x => x.replaceAll("[A-Za-z0-9 \"'()?!éèîàÀÉÈç+]", ""))
      .flatMap(_.toCharArray)
      .map(w => (w, 1))
      .reduceByKey(_ + _)
      .first()._1
      .toString
  }

  /** Get domain name
    *
    * @param path : file path
    * @return the domain name
    */
  def getDomainName(path: Path) : String = {
    path.getName
  }

  /** Get domain directory name
    *
    * @param path : file path
    * @return the domain directory name
    */
  def getDomainDirectoryName(path: Path): String = {
    path.toString.replace(path.getName, "")
  }

  /** Get schema name
    *
    * @param path : file path
    * @return the schema name
    */
  def getSchemaName(path: Path) : String = {
    val fileName = path.getName

    if(fileName.contains("."))
      fileName.split("\\.").head
    else
      fileName
  }

  /** Get domain pattern
    *
    * @param path : file path
    * @return the domain pattern
    */
  def getSchemaPattern(path: Path) : String = {
    path.getName
  }

  /** Get header option
    *
    * @return the header option
    */
  def getWithHeader : Boolean = {
    false
  }

  def getMultiline : Boolean = {
    false
  }

  def getQuote: String = {
    "\""
  }

  def getEscape: String = {
    "\\"
  }

  def getWriteMode : String = {
    "OVERWRITE"
  }

  /**
    *
    * @param datasetInit : created dataset without specifying format
    * @param path : file path
    * @return
    */
  def createDataFrameWithFormat(datasetInit: Dataset[String],path: Path): DataFrame = {
    val formatFile = getFormatFile(datasetInit, getExtensionFile(path))

    formatFile match {
      case "JSON" | "ARRAY_JSON" => session.read
        .format("json")
        .option("inferSchema", false)
        .load(path.toString)

      case "DSV" => session.read
        .format("com.databricks.spark.csv")
        .option("header", getWithHeader)
        .option("inferSchema", false)
        .option("delimiter", getSeparator(datasetInit))
        .option("quote", getQuote)
        .option("escape", getEscape)
        .option("parserLib", "UNIVOCITY")
        .load(path.toString)
    }
  }

  override def name: String = "InferSchema"

  /**
    * Just to force any spark job to implement its entry point using within the "run" method
    *
    * @return : Spark Session used for the job
    */
  override def run(): SparkSession = session

  val path = new Path("/tmp/ty/toy.json")
  val rf = readFile(path)
  val cdf = createDataFrameWithFormat(rf, path)

  val mode = getMode

  val format = getFormatFile(rf,getExtensionFile(path))

  val multiline = getMultiline

  val array = if(format == "ARRAY_JSON") true else false

  val withHeader = getWithHeader

  val separator = getSeparator(rf)

  val quote = getQuote

  val escape = getEscape

  val writeMode = getWriteMode

  val inferSchema = new InferSchemaHandler(cdf)

  val attributes: List[Attribute] = inferSchema.createAttributes(cdf.schema)
  val metadata = inferSchema.createMetaData(mode,format,multiline,array,withHeader,separator,quote,escape,writeMode,None,false,None)

  val schema = inferSchema.createSchema(getSchemaName(path),Pattern.compile(getSchemaPattern(path)),attributes,metadata)

  val domain = inferSchema.createDomain(getDomainName(path),getDomainDirectoryName(path),None, List(schema))

  inferSchema.generateYaml(domain, "/tmp/res.yml")

}
