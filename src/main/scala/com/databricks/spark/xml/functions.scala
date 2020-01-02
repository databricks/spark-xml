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
import org.apache.spark.sql.{Column, Dataset, Row}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}

import com.databricks.spark.xml.parsers.StaxXmlParser
import com.databricks.spark.xml.util.InferSchema

/**
 * Support functions for working with XML columns directly.
 */
object functions {

  /**
   * Infers the schema of XML documents as strings.
   *
   * @param ds Dataset of XML strings
   * @param options additional XML parsing options
   * @return inferred schema for XML
   */
  @Experimental
  def schema_of_xml(ds: Dataset[String], options: Map[String, String] = Map.empty): StructType =
    InferSchema.infer(ds.rdd, XmlOptions(options))

  /**
   * Infers the schema of XML documents when inputs are arrays of strings, each an XML doc.
   *
   * @param ds Dataset of XML strings
   * @param options additional XML parsing options
   * @return inferred schema for XML. Will be an ArrayType[StructType].
   */
  @Experimental
  def schema_of_xml_array(ds: Dataset[Array[String]],
      options: Map[String, String] = Map.empty): ArrayType =
    ArrayType(InferSchema.infer(ds.rdd.flatMap(a => a), XmlOptions(options)))

  /**
   * Parses a column containing a XML string into a `StructType` with the specified schema.
   *
   * @param e a string column containing XML data
   * @param schema the schema to use when parsing the XML string. Must be a StructType if
   *   column is string-valued, or ArrayType[StructType] if column is an array of strings
   * @param options key-value pairs that correspond to those supported by [[XmlOptions]]
   */
  @Experimental
  def from_xml(e: Column, schema: DataType, options: Map[String, String] = Map.empty): Column = {
    val expr = CatalystSqlParser.parseExpression(e.toString())
    new Column(XmlDataToCatalyst(expr, schema, XmlOptions(options)))
  }

  /**
   * @param xml XML document to parse, as string
   * @param schema the schema to use when parsing the XML string
   * @param options key-value pairs that correspond to those supported by [[XmlOptions]]
   * @return [[Row]] representing the parsed XML structure
   */
  @Experimental
  def from_xml_string(xml: String, schema: StructType,
       options: Map[String, String] = Map.empty): Row = {
    StaxXmlParser.parseColumn(xml, schema, XmlOptions(options))
  }

}
