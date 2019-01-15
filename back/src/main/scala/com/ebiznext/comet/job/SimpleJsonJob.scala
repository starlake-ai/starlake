package com.ebiznext.comet.job

import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame


/**
  * Parse a simple one level json file. Complex types such as arrays & maps are not supported.
  * @param domain         : Input Dataset Domain
  * @param schema         : Input Dataset Schema
  * @param types          : List of globally defined types
  * @param path           : Input dataset path
  * @param storageHandler : Storage Handler
  */
class SimpleJsonJob(domain: Domain, schema: Schema, types: List[Type], path: Path, storageHandler: StorageHandler) extends DsvJob(domain, schema, types, path, storageHandler) {
  override def loadDataSet(): DataFrame = {
    val df =
      if (metadata.getArray()) {
        val jsonRDD = session.sparkContext.wholeTextFiles(path.toString).map(x => x._2)
        session.read.json(jsonRDD)
      } else {
        session.read.option("multiline", metadata.getMultiline().toString).json(path.toString)
      }
    df.show()
    if (metadata.withHeader.getOrElse(false)) {
      val datasetHeaders: List[String] = df.columns.toList.map(cleanHeaderCol)
      val (_, drop) = intersectHeaders(datasetHeaders, schemaHeaders)
      if (drop.nonEmpty) {
        df.drop(drop: _*)
      }
      else
        df
    } else
      df
  }
}
