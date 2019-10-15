package com.ebiznext.comet.job.ingest

import com.ebiznext.comet.schema.handlers.StorageHandler
import com.ebiznext.comet.schema.model.{Domain, Schema, Type}
import com.ebiznext.comet.utils.SparkJob
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import scala.reflect.runtime.universe
import scala.util.Try

trait ChewerJob extends SparkJob {
  var domain: Domain
  var schema: Schema
  var types: List[Type]
  var path: List[Path]
  var storageHandler: StorageHandler

  def run(
    domain: Domain,
    schema: Schema,
    types: List[Type],
    path: List[Path],
    storageHandler: StorageHandler
  ): Try[SparkSession] = {
    this.domain = domain
    this.schema = schema
    this.types = types
    this.path = path
    this.storageHandler = storageHandler
    run()
  }
}

object ChewerJob {

  def run(
    objName: String,
    domain: Domain,
    schema: Schema,
    types: List[Type],
    path: List[Path],
    storageHandler: StorageHandler
  ): Unit = {
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val module = runtimeMirror.staticModule(objName)
    val obj: universe.ModuleMirror = runtimeMirror.reflectModule(module)
    val chewer = obj.instance.asInstanceOf[ChewerJob]
    chewer.run(domain, schema, types, path, storageHandler);
  }
}
