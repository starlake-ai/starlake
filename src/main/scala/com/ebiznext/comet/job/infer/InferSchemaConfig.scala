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
package com.ebiznext.comet.job.infer

import scopt.OParser

case class InferConfig(
                        domainName: String = "",
                        schemaName: String = "",
                        inputPath: String = "",
                        outputPath: String = "",
                        header: Option[Boolean] = Some(false)
                      )

object InferSchemaConfig{

  /**
    *
    * @param args args list passed from command line
    * @return Option of case class InferConfig.
    */
  def parse(args: Seq[String]): Option[InferConfig] = {
    val builder = OParser.builder[InferConfig]

    val parser: OParser[Unit, InferConfig] = {
      import builder._
      OParser.sequence(
        programName("comet"),
        head("comet", "1.x"),
        opt[String]("domain")
          .action((x, c) => c.copy(domainName = x))
          .required()
          .text("Domain Name"),
        opt[String]("schema")
          .action((x, c) => c.copy(schemaName = x))
          .required()
          .text("Domain Name"),
        opt[String]("input")
          .action((x, c) => c.copy(inputPath = x))
          .required()
          .text("Input Path"),
        opt[String]("output")
          .action((x, c) => c.copy(outputPath = x))
          .required()
          .text("Output Path"),
        opt[Option[Boolean]]("with-header")
          .action((x, c) => c.copy(header = x))
          .optional()
          .text("Does the file contain a header")
      )
    }
    OParser.parse(parser, args, InferConfig())

  }
}
