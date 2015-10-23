/*
 * Copyright 2014 Apache
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
package org.apache.spark.xml


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.StructType
import org.apache.spark.xml.util.{ParserLibs, ParseModes, TextFile}

/**
 * A collection of static functions for working with XML files in Spark SQL
 */
class XmlParser extends Serializable {

  private var useHeader: Boolean = false
  private var delimiter: Character = ','
  private var quote: Character = '"'
  private var escape: Character = null
  private var comment: Character = '#'
  private var schema: StructType = null
  private var parseMode: String = ParseModes.DEFAULT
  private var ignoreLeadingWhiteSpace: Boolean = false
  private var ignoreTrailingWhiteSpace: Boolean = false
  private var treatEmptyValuesAsNulls: Boolean = false
  private var parserLib: String = ParserLibs.DEFAULT
  private var charset: String = TextFile.DEFAULT_CHARSET.name()
  private var inferSchema: Boolean = false

  def withUseHeader(flag: Boolean): XmlParser = {
    this.useHeader = flag
    this
  }

  def withDelimiter(delimiter: Character): XmlParser = {
    this.delimiter = delimiter
    this
  }

  def withQuoteChar(quote: Character): XmlParser = {
    this.quote = quote
    this
  }

  def withSchema(schema: StructType): XmlParser = {
    this.schema = schema
    this
  }

  def withParseMode(mode: String): XmlParser = {
    this.parseMode = mode
    this
  }

  def withEscape(escapeChar: Character): XmlParser = {
    this.escape = escapeChar
    this
  }

  def withComment(commentChar: Character) : XmlParser = {
    this.comment = commentChar
    this
  }

  def withIgnoreLeadingWhiteSpace(ignore: Boolean): XmlParser = {
    this.ignoreLeadingWhiteSpace = ignore
    this
  }

  def withIgnoreTrailingWhiteSpace(ignore: Boolean): XmlParser = {
    this.ignoreTrailingWhiteSpace = ignore
    this
  }

  def withTreatEmptyValuesAsNulls(treatAsNull: Boolean): XmlParser = {
    this.treatEmptyValuesAsNulls = treatAsNull
    this
  }

  def withParserLib(parserLib: String): XmlParser = {
    this.parserLib = parserLib
    this
  }

  def withCharset(charset: String): XmlParser = {
    this.charset = charset
    this
  }

  def withInferSchema(inferSchema: Boolean): XmlParser = {
    this.inferSchema = inferSchema
    this
  }

  /** Returns a Schema RDD for the given XML path. */
  @throws[RuntimeException]
  def xmlFile(sqlContext: SQLContext, path: String): DataFrame = {
    val relation: XmlRelation = XmlRelation(
      () => TextFile.withCharset(sqlContext.sparkContext, path, charset),
      Some(path),
      useHeader,
      delimiter,
      quote,
      escape,
      comment,
      parseMode,
      parserLib,
      ignoreLeadingWhiteSpace,
      ignoreTrailingWhiteSpace,
      treatEmptyValuesAsNulls,
      schema,
      inferSchema)(sqlContext)
    sqlContext.baseRelationToDataFrame(relation)
  }

  def xmlRdd(sqlContext: SQLContext, xmlRDD: RDD[String]): DataFrame = {
    val relation: XmlRelation = XmlRelation(
      () => xmlRDD,
      None,
      useHeader,
      delimiter,
      quote,
      escape,
      comment,
      parseMode,
      parserLib,
      ignoreLeadingWhiteSpace,
      ignoreTrailingWhiteSpace,
      treatEmptyValuesAsNulls,
      schema,
      inferSchema)(sqlContext)
    sqlContext.baseRelationToDataFrame(relation)
  }
}
