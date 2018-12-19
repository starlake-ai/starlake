package com.ebiznext.comet.job

import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model.SchemaModel
import com.ebiznext.comet.schema.model.SchemaModel.{Domain, Metadata, Type}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame

class JsonJob(domain: Domain, schema: SchemaModel.Schema, types: List[Type], metadata: Metadata, path: Path, storageHandler: StorageHandler) extends DsvJob(domain, schema, types, metadata, path, storageHandler) {
  override def loadDataSet(): DataFrame = {
    val df = session.read.json(path.toString)
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
