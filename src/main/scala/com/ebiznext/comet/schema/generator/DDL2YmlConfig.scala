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
package com.ebiznext.comet.schema.generator

/**
  *
  * @param config : JDBC Configuration to use as defined in the connection section in the application.conf
  * @param catalog : Database catalog name, optional.
  * @param schema : Database schema to use, required.
  * @param tables : Tables to extract. Nil if all tables should be extracted
  * @param tableTypes : Table types to extract
  */
case class JDBCSchema(
  config: String,
  catalog: Option[String] = None,
  schema: String = "",
  tables: List[JDBCTable] = Nil,
  tableTypes: List[String] = List(
    "TABLE",
    "VIEW",
    "SYSTEM TABLE",
    "GLOBAL TEMPORARY",
    "LOCAL TEMPORARY",
    "ALIAS",
    "SYNONYM"
  )
)

/**
  *
  * @param table : Table name (case insensitive)
  * @param columns : List of columns (case insensitive). Nil  if all columns should be extracted
  */
case class JDBCTable(table: String, columns: List[String] = Nil)
