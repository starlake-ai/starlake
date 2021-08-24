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

import com.ebiznext.comet.utils.CliConfig
import scopt.OParser

case class JDBCSchemas(
  jdbcSchemas: List[JDBCSchema]
)

/** @param connection
  *   : JDBC Configuration to use as defined in the connection section in the application.conf
  * @param catalog
  *   : Database catalog name, optional.
  * @param schema
  *   : Database schema to use, required.
  * @param tables
  *   : Tables to extract. Nil if all tables should be extracted
  * @param tableTypes
  *   : Table types to extract
  */

case class JDBCSchema(
  connection: String,
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
  ),
  template: Option[String] = None
)

/** @param name
  *   : Table name (case insensitive)
  * @param columns
  *   : List of columns (case insensitive). Nil if all columns should be extracted
  */
case class JDBCTable(name: String, columns: Option[List[String]])

case class DDL2YmlConfig(
  jdbcMapping: String = "",
  outputDir: String = "",
  ymlTemplate: Option[String] = None
)

object DDL2YmlConfig extends CliConfig[DDL2YmlConfig] {

  val parser: OParser[Unit, DDL2YmlConfig] = {
    val builder = OParser.builder[DDL2YmlConfig]
    import builder._
    OParser.sequence(
      programName("comet ddl2yml"),
      head("comet", "ddl2yml", "[options]"),
      note(""),
      opt[String]("jdbc-mapping")
        .action((x, c) => c.copy(jdbcMapping = x))
        .required()
        .text("Database tables & connection info"),
      opt[String]("output-dir")
        .action((x, c) => c.copy(outputDir = x))
        .required()
        .text("Where to output YML files"),
      opt[String]("yml-template")
        .action((x, c) => c.copy(ymlTemplate = Some(x)))
        .optional()
        .text("YML template to use YML metadata")
    )
  }

  /** @param args
    *   args list passed from command line
    * @return
    *   Option of case class DDL2YmlConfig.
    */
  def parse(args: Seq[String]): Option[DDL2YmlConfig] =
    OParser.parse(parser, args, DDL2YmlConfig())
}
