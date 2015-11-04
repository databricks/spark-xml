/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.xml

import java.io.IOException

import org.apache.spark.sql.xml.parsers.stax._
import org.apache.spark.sql.xml.util.{InferSchema, ParseModes}

import scala.collection.JavaConversions._
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types._

case class XmlRelation protected[spark] (
    baseRDD: () => RDD[String],
    location: Option[String],
    parseMode: String,
    rootTag: String,
    samplingRatio: Double,
    includeAttributeFlag: Boolean,
    treatEmptyValuesAsNulls: Boolean,
    userSchema: StructType = null)(@transient val sqlContext: SQLContext)
  extends BaseRelation
  with TableScan {

  private val logger = LoggerFactory.getLogger(XmlRelation.getClass)

  // Parse mode flags
  if (!ParseModes.isValidMode(parseMode)) {
    logger.warn(s"$parseMode is not a valid parse mode. Using ${ParseModes.DEFAULT}.")
  }

  private val failFast = ParseModes.isFailFastMode(parseMode)
  private val dropMalformed = ParseModes.isDropMalformedMode(parseMode)
  private val permissive = ParseModes.isPermissiveMode(parseMode)

  override val schema: StructType = {
    Option(userSchema).getOrElse {
      InferSchema(
        StaxXmlPartialSchemaParser(
          baseRDD(),
          rootTag,
          samplingRatio)(sqlContext))}
  }

  override def buildScan: RDD[Row] = {
    StaxXmlParser(
      baseRDD(),
      schema,
      rootTag)(sqlContext)
  }
}
