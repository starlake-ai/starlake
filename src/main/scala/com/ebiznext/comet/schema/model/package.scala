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

package com.ebiznext.comet.schema

import scala.collection.mutable

package object model {

  /**
    * Utility to extract duplicates and their number of occurrences
    *
    * @param values       : Liste of strings
    * @param errorMessage : Error Message that should contains placeholders for the value(%s) and number of occurrences (%d)
    * @return List of tuples contains for ea  ch duplicate the number of occurrences
    */
  def duplicates(values: List[String], errorMessage: String): Either[List[String], Boolean] = {
    val errorList: mutable.MutableList[String] = mutable.MutableList.empty
    val duplicates = values.groupBy(identity).mapValues(_.size).filter { case (key, size) =>
      size > 1
    }
    duplicates.foreach { case (key, size) =>
      errorList += errorMessage.format(key, size)
    }
    if (errorList.nonEmpty)
      Left(errorList.toList)
    else
      Right(true)
  }

  def combine(
    errors1: Either[List[String], Boolean],
    errors2: Either[List[String], Boolean]*
  ): Either[List[String], Boolean] = {
    val allErrors = errors1 :: List(errors2: _*)
    val errors = allErrors.collect { case Left(err) =>
      err
    }.flatten
    if (errors.isEmpty) Right(true) else Left(errors)
  }
}
