package org.apache.spark.sql

trait DatasetLogging {
  implicit class DatasetHelper[T](ds: Dataset[T]) {
    def showString(numRows: Int = 20, truncate: Int = 20, vertical: Boolean = false): String =
      ds.asInstanceOf[org.apache.spark.sql.classic.Dataset[T]]
        .showString(numRows, truncate, vertical)

    def schemaString(): String = ds.schema.treeString
  }
}
