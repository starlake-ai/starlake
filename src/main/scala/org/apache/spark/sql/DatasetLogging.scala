package org.apache.spark.sql

trait DatasetLogging {
  implicit class DatasetHelper[T](ds: Dataset[T]) {
    def showString(numRows: Int = 20, truncate: Int = 20, vertical: Boolean = false): String =
      ds.showString(numRows, truncate, vertical)

    def schemaString(level: Int = Int.MaxValue): String = ds.schema.treeString(level)
  }
}
