package com.ebiznext.comet.schema.model

import java.sql.Timestamp

/**
  * Contains classes used to describe rejected records.
  * Recjected records are stored in parquet file in teh rejected area.
  * A reject row contains
  *   - the list of columns and for each column wether it has been accepted or not.
  * A row is rejected if at least one of its column is rejected
  */
object Rejection {

  /** Col information and status of parsing
    *
    * @param colData : Column value found in row
    * @param colName : Column source name
    * @param typeName : column semantic type name
    * @param pattern : applied pattern
    * @param success : true if column is valid, false otherwise
    */
  case class ColInfo(colData: String, colName: String, typeName: String, pattern: String, success: Boolean)


  /**
    * Rejected Row information
    * @param timestamp : time of parsing for this row
    * @param colInfos : column parsing results for this row
    */
  case class RowInfo(timestamp: Timestamp, colInfos: List[ColInfo])

  /**
    * ColResult after parsing
    * @param colInfo : Col info
    * @param sparkValue : Spark Type as recognized by catalyst
    */
  case class ColResult(colInfo: ColInfo, sparkValue: Any)

  /**
    *
    * @param colResults
    */
  case class RowResult(colResults: List[ColResult]) {
    def isRejected: Boolean = colResults.exists(!_.colInfo.success)

    def isAccepted: Boolean = !isRejected
  }

}
