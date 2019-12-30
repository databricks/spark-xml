/*
 * Copyright 2019 Databricks
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

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.StructType

import com.databricks.spark.xml.util.InferSchema

/**
 * Support functions for working with XML columns directly.
 */
object functions {

  /**
   * Infers the schema of XML documents as strings.
   *
   * @param ds Dataset of XML strings
   * @return inferred schema for XML
   */
  @Experimental
  def schema_of_xml(ds: Dataset[String]): StructType =
    inferSchema(ds, Map.empty[String, String])

  /**
   * Infers the schema of XML documents as strings.
   *
   * @param ds Dataset of XML strings
   * @param options additional XML parsing options
   * @return inferred schema for XML
   */
  @Experimental
  def schema_of_xml(ds: Dataset[String], options: Map[String, String]): StructType =
    InferSchema.infer(ds.rdd, XmlOptions(options))

  /**
   * Parses a column containing a XML string into a `StructType` with the specified schema.
   *
   * @param e a string column containing XML data
   * @param schema the schema to use when parsing the XML string
   */
  @Experimental
  def from_xml(e: Column, schema: StructType): Column =
    from_xml(e, schema, Map.empty)

  /**
   * Parses a column containing a XML string into a `StructType` with the specified schema.
   *
   * @param e a string column containing XML data
   * @param schema the schema to use when parsing the XML string
   * @param options key-value pairs that correspond to those supported by [[XmlOptions]]
   */
  @Experimental
  def from_xml(e: Column, schema: StructType, options: Map[String, String]): Column = {
    val expr = CatalystSqlParser.parseExpression(e.toString())
    new Column(XmlDataToCatalyst(expr, schema, XmlOptions(options)))
  }

}
