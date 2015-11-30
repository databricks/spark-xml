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
package com.databricks.spark.xml

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.StructType
import com.databricks.spark.xml.util.{ParseModes, XmlFile}


/**
 * A collection of static functions for working with XML files in Spark SQL
 */
class XmlReader extends Serializable {

  private var charset: String = XmlFile.DEFAULT_CHARSET.name()
  private var parseMode: String = ParseModes.DEFAULT
  private var rootTag: String = null
  private var samplingRatio: Double = 1.0
  private var excludeAttributeFlag: Boolean = false
  private var treatEmptyValuesAsNulls: Boolean = false
  private var schema: StructType = null

  def withCharset(charset: String): XmlReader = {
    this.charset = charset
    this
  }

  def withParseMode(mode: String): XmlReader = {
    this.parseMode = mode
    this
  }

  def withRootTag(rootTag: String): XmlReader = {
    this.rootTag = rootTag
    this
  }

  def withSamplingRatio(samplingRatio: Double): XmlReader = {
    this.samplingRatio = samplingRatio
    this
  }

  def withIncludeAttributeFlag(exclude: Boolean): XmlReader = {
    this.excludeAttributeFlag = exclude
    this
  }

  def withTreatEmptyValuesAsNulls(treatAsNull: Boolean): XmlReader = {
    this.treatEmptyValuesAsNulls = treatAsNull
    this
  }

  def withSchema(schema: StructType): XmlReader = {
    this.schema = schema
    this
  }

  /** Returns a Schema RDD for the given XML path. */
  @throws[RuntimeException]
  def xmlFile(sqlContext: SQLContext, path: String): DataFrame = {
    val relation: XmlRelation = XmlRelation(
      () => XmlFile.withCharset(sqlContext.sparkContext, path, charset, rootTag),
      Some(path),
      parseMode,
      samplingRatio,
      excludeAttributeFlag,
      treatEmptyValuesAsNulls,
      schema)(sqlContext)
    sqlContext.baseRelationToDataFrame(relation)
  }

  def xmlRdd(sqlContext: SQLContext, xmlRDD: RDD[String]): DataFrame = {
    val relation: XmlRelation = XmlRelation(
      () => xmlRDD,
      None,
      parseMode,
      samplingRatio,
      excludeAttributeFlag,
      treatEmptyValuesAsNulls,
      schema)(sqlContext)
    sqlContext.baseRelationToDataFrame(relation)
  }
}
