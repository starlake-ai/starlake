package com.ebiznext.comet.job

import com.ebiznext.comet.config.{DatasetArea, HiveArea}
import com.ebiznext.comet.config.HiveArea.business
import com.ebiznext.comet.schema.model.SchemaModel.Write
import org.apache.hadoop.fs.Path

class AutoBusinessJob(override val name: String, sql: String, domain: String, dataset: String, write:Write) extends SparkJob {
  override def run(args: Array[String] = Array()): Unit = {
    val hiveDB = HiveArea.area(domain, business)
    val tableName = dataset
    session.sql(s"create database if not exists $hiveDB")
    session.sql(s"use $hiveDB")
    session.sql(s"drop table if exists $tableName")
    val dataframe = session.sql(sql)
    val targetPath = new Path(DatasetArea.path(domain, HiveArea.business.value), dataset)
    dataframe.write.mode(write.toSaveMode).option("path", targetPath.toString).saveAsTable(tableName)
  }
}
