package com.ebiznext.comet.job

import com.ebiznext.comet.config.HiveArea.business
import com.ebiznext.comet.config.{DatasetArea, HiveArea}
import com.ebiznext.comet.schema.model.SchemaModel.BusinessTask
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

class AutoBusinessJob(override val name: String, task : BusinessTask) extends SparkJob {
  def run(args: Array[String] = Array()): SparkSession = {
    task.presql.getOrElse(Nil).foreach(session.sql)
    val hiveDB = HiveArea.area(task.domain, business)
    val tableName = task.dataset
    session.sql(s"create database if not exists $hiveDB")
    session.sql(s"use $hiveDB")
    session.sql(s"drop table if exists $tableName")
    val dataframe = session.sql(task.sql)
    val targetPath = new Path(DatasetArea.path(task.domain, HiveArea.business.value), task.dataset)
    val partitionedDF = partitionedDatasetWriter(dataframe, task.partition)
    partitionedDF.mode(task.write.toSaveMode).option("path", targetPath.toString).saveAsTable(tableName)
    task.postsql.getOrElse(Nil).foreach(session.sql)
    session
  }
}
