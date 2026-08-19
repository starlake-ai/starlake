package org.apache.spark.sql

trait DatasetLogging {

  /** Spark 4 moved the Dataset implementation to `org.apache.spark.sql.classic`. `showString` lives
    * there only, so it must be reached through the concrete class: calling it on the abstract
    * `Dataset` resolves back to this very method. Connect datasets expose no equivalent, hence the
    * schema-only fallback.
    */
  implicit class DatasetHelper[T](ds: Dataset[T]) {
    def showString(numRows: Int = 20, truncate: Int = 20, vertical: Boolean = false): String =
      ds match {
        case classicDs: classic.Dataset[_] => classicDs.showString(numRows, truncate, vertical)
        case _                             => ds.schema.treeString
      }

    def schemaString(): String = ds.schema.treeString
  }
}
