/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package com.ebiznext.comet.schema.model

import java.sql.Timestamp

/** Contains classes used to describe rejected records.
  * Recjected records are stored in parquet file in teh rejected area.
  * A reject row contains
  *   - the list of columns and for each column wether it has been accepted or not.
  * A row is rejected if at least one of its column is rejected
  */
object Rejection {

  /** Col information and status of parsing
    *
    * @param colData  : Column value found in row
    * @param colName  : Column source name
    * @param typeName : column semantic type name
    * @param pattern  : applied pattern
    * @param success  : true if column is valid, false otherwise
    */
  case class ColInfo(
    colData: Option[String],
    colName: String,
    typeName: String,
    pattern: String,
    success: Boolean
  ) {

    override def toString: String = {
      val failMsg = if (success) "success" else "failure"
      //s"$failMsg $colName:$typeName($pattern) = $colData"
      s"""$colName,$typeName,"$pattern","$colData",$failMsg"""
    }
  }

  /** Rejected Row information
    *
    * @param timestamp : time of parsing for this row
    * @param colInfos  : column parsing results for this row
    */
  case class RowInfo(timestamp: Timestamp, colInfos: List[ColInfo]) {

    override def toString: String = {
      colInfos.map(_.toString).mkString("\n")

    }
  }

  /** ColResult after parsing
    *
    * @param colInfo    : Col info
    * @param sparkValue : Spark Type as recognized by catalyst
    */
  case class ColResult(colInfo: ColInfo, sparkValue: Any) {
    override def toString: String = colInfo.toString
  }

  /** @param colResults
    */
  case class RowResult(colResults: List[ColResult]) {
    def isRejected: Boolean = colResults.exists(!_.colInfo.success)
    def isAccepted: Boolean = !isRejected

    override def toString: String = colResults.map(_.toString).mkString("\n")
  }

}
