package com.ebiznext.comet.job

import com.ebiznext.comet.config.HiveArea.business
import com.ebiznext.comet.config.{DatasetArea, HiveArea, Settings}
import com.ebiznext.comet.schema.model.SchemaModel.BusinessTask
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

class AutoBusinessJob(override val name: String, task : BusinessTask) extends SparkJob {
  def run(args: Array[String] = Array()): SparkSession = {
    if(Settings.comet.hive) {
      task.presql.getOrElse(Nil).foreach(session.sql)
      val hiveDB = HiveArea.area(task.domain, business)
      val tableName = task.dataset
      val fullTableName = s"$hiveDB.$tableName"
      session.sql(s"create database if not exists $hiveDB")
      session.sql(s"use $hiveDB")
      session.sql(s"drop table if exists $tableName")
      val dataframe = session.sql(task.sql)
      val targetPath = new Path(DatasetArea.path(task.domain, HiveArea.business.value), task.dataset)
      val partitionedDF = partitionedDatasetWriter(dataframe, task.partition)
      partitionedDF.mode(task.write.toSaveMode).option("path", targetPath.toString).saveAsTable(fullTableName)
      val allCols = session.table(fullTableName).columns.mkString(",")
      val analyzeTable = s"ANALYZE TABLE $fullTableName COMPUTE STATISTICS FOR COLUMNS $allCols"
      if (session.version.substring(0, 3).toDouble >= 2.4)
        session.sql(analyzeTable)
      task.postsql.getOrElse(Nil).foreach(session.sql)
    }
    session
  }
}
