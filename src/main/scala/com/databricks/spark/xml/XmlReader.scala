/*
 * Copyright 2014 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import com.databricks.spark.xml.util.XmlFile

/**
 * A collection of static functions for working with XML files in Spark SQL
 */
class XmlReader extends Serializable {
  private var parameters = collection.mutable.Map.empty[String, String]
  private var schema: StructType = null

  def withCharset(charset: String): XmlReader = {
    parameters += ("charset" -> charset)
    this
  }

  def withCompression(codec: String): XmlReader = {
    parameters += ("codec" -> codec)
    this
  }

  def withRowTag(rowTag: String): XmlReader = {
    parameters += ("rowTag" -> rowTag)
    this
  }

  def withSamplingRatio(samplingRatio: Double): XmlReader = {
    parameters += ("samplingRatio" -> samplingRatio.toString)
    this
  }

  def withExcludeAttribute(exclude: Boolean): XmlReader = {
    parameters += ("excludeAttribute" -> exclude.toString)
    this
  }

  def withTreatEmptyValuesAsNulls(treatAsNull: Boolean): XmlReader = {
    parameters += ("treatEmptyValuesAsNulls" -> treatAsNull.toString)
    this
  }

  def withFailFast(failFast: Boolean): XmlReader = {
    parameters += ("failFast" -> failFast.toString)
    this
  }

  def withParseMode(valueTag: String): XmlReader = {
    parameters += ("mode" -> valueTag)
    this
  }

  def withAttributePrefix(attributePrefix: String): XmlReader = {
    parameters += ("attributePrefix" -> attributePrefix)
    this
  }

  def withValueTag(valueTag: String): XmlReader = {
    parameters += ("valueTag" -> valueTag)
    this
  }

  def withColumnNameOfCorruptRecord(name: String): XmlReader = {
    parameters += ("columnNameOfCorruptRecord" -> name)
    this
  }

  def withSchema(schema: StructType): XmlReader = {
    this.schema = schema
    this
  }

  /** Returns a Schema RDD for the given XML path. */
  @throws[RuntimeException]
  def xmlFile(sqlContext: SQLContext, path: String): DataFrame = {
    // We need the `charset` and `rowTag` before creating the relation.
    val (charset, rowTag) = {
      val options = XmlOptions(parameters.toMap)
      (options.charset, options.rowTag)
    }
    val relation: XmlRelation = XmlRelation(
      () => XmlFile.withCharset(sqlContext.sparkContext, path, charset, rowTag),
      Some(path),
      parameters.toMap,
      schema)(sqlContext)
    sqlContext.baseRelationToDataFrame(relation)
  }

  def xmlRdd(sqlContext: SQLContext, xmlRDD: RDD[String]): DataFrame = {
    val relation: XmlRelation = XmlRelation(
      () => xmlRDD,
      None,
      parameters.toMap,
      schema)(sqlContext)
    sqlContext.baseRelationToDataFrame(relation)
  }
}
