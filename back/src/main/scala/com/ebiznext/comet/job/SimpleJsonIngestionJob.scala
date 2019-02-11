package com.ebiznext.comet.job

import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame

/**
  * Parse a simple one level json file. Complex types such as arrays & maps are not supported.
  * Use JsonIngestionJob instead.
  * This class is for simpel json only that makes it way faster.
  *
  * @param domain         : Input Dataset Domain
  * @param schema         : Input Dataset Schema
  * @param types          : List of globally defined types
  * @param path           : Input dataset path
  * @param storageHandler : Storage Handler
  */
class SimpleJsonIngestionJob(
  domain: Domain,
  schema: Schema,
  types: List[Type],
  path: Path,
  storageHandler: StorageHandler
) extends DsvIngestionJob(domain, schema, types, path, storageHandler) {
  override def loadDataSet(): DataFrame = {
    val df =
      if (metadata.isArray()) {
        val jsonRDD =
          session.sparkContext.wholeTextFiles(path.toString).map(x => x._2)
        session.read.json(jsonRDD)
      } else {
        session.read
          .option("multiline", metadata.getMultiline())
          .json(path.toString)
      }
    df.printSchema()
    if (df.columns.exists(col => col == "_corrupt_record"))
      throw new Exception(
        s"Invalid JSON File: ${path.toString}. SIMPLE_JSON require a valid json file "
      )
    if (metadata.withHeader.getOrElse(false)) {
      val datasetHeaders: List[String] = df.columns.toList.map(cleanHeaderCol)
      val (_, drop) = intersectHeaders(datasetHeaders, schemaHeaders)
      if (drop.nonEmpty) {
        df.drop(drop: _*)
      } else
        df
    } else
      df
  }
}
