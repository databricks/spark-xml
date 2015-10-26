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
package org.apache.spark.sql

import org.apache.commons.xml.XMLFormat
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.sql.xml.util.TextFile

import org.apache.spark.sql.{DataFrame, SQLContext}
import TextFile

package object xml {

  val defaultXmlFormat =
    XMLFormat.DEFAULT.withRecordSeparator(System.getProperty("line.separator", "\n"))

  /**
   * Adds a method, `xmlFile`, to SQLContext that allows reading XML data.
   */
  implicit class XmlContext(sqlContext: SQLContext) extends Serializable{
    def xmlFile(
        filePath: String,
        useHeader: Boolean = true,
        delimiter: Char = ',',
        quote: Char = '"',
        escape: Character = null,
        comment: Character = null,
        mode: String = "PERMISSIVE",
        parserLib: String = "COMMONS",
        ignoreLeadingWhiteSpace: Boolean = false,
        ignoreTrailingWhiteSpace: Boolean = false,
        charset: String = TextFile.DEFAULT_CHARSET.name(),
        inferSchema: Boolean = false): DataFrame = {
      val xmlRelation = XmlRelation(
        () => TextFile.withCharset(sqlContext.sparkContext, filePath, charset),
        location = Some(filePath),
        useHeader = useHeader,
        delimiter = delimiter,
        quote = quote,
        escape = escape,
        comment = comment,
        parseMode = mode,
        parserLib = parserLib,
        ignoreLeadingWhiteSpace = ignoreLeadingWhiteSpace,
        ignoreTrailingWhiteSpace = ignoreTrailingWhiteSpace,
        treatEmptyValuesAsNulls = false,
        inferXmlSchema = inferSchema)(sqlContext)
      sqlContext.baseRelationToDataFrame(xmlRelation)
    }

    def xmlFile(
        filePath: String,
        useHeader: Boolean = true,
        parserLib: String = "COMMONS",
        ignoreLeadingWhiteSpace: Boolean = false,
        ignoreTrailingWhiteSpace: Boolean = false,
        charset: String = TextFile.DEFAULT_CHARSET.name(),
        inferSchema: Boolean = false): DataFrame = {
      val xmlRelation = XmlRelation(
        () => TextFile.withCharset(sqlContext.sparkContext, filePath, charset),
        location = Some(filePath),
        useHeader = useHeader,
        delimiter = '\t',
        quote = '"',
        escape = '\\',
        comment = '#',
        parseMode = "PERMISSIVE",
        parserLib = parserLib,
        ignoreLeadingWhiteSpace = ignoreLeadingWhiteSpace,
        ignoreTrailingWhiteSpace = ignoreTrailingWhiteSpace,
        treatEmptyValuesAsNulls = false,
        inferXmlSchema = inferSchema)(sqlContext)
      sqlContext.baseRelationToDataFrame(xmlRelation)
    }
  }

  implicit class XmlSchemaRDD(dataFrame: DataFrame) {

    /**
     * Saves DataFrame as xml files. By default uses ',' as delimiter, and includes header line.
     */
    def saveAsXmlFile(path: String, parameters: Map[String, String] = Map(),
                      compressionCodec: Class[_ <: CompressionCodec] = null): Unit = {
      // TODO(hossein): For nested types, we may want to perform special work
      val delimiter = parameters.getOrElse("delimiter", ",")
      val delimiterChar = if (delimiter.length == 1) {
        delimiter.charAt(0)
      } else {
        throw new Exception("Delimiter cannot be more than one character.")
      }

      val escape = parameters.getOrElse("escape", null)
      val escapeChar: Character = if (escape == null) {
        null
      } else if (escape.length == 1) {
        escape.charAt(0)
      } else {
        throw new Exception("Escape character cannot be more than one character.")
      }

      val quote = parameters.getOrElse("quote", "\"")
      val quoteChar: Character = if (quote == null) {
        null
      } else if (quote.length == 1) {
        quote.charAt(0)
      } else {
        throw new Exception("Quotation cannot be more than one character.")
      }

      val nullValue = parameters.getOrElse("nullValue", "null")

      val xmlFormat = defaultXmlFormat
        .withDelimiter(delimiterChar)
        .withQuote(quoteChar)
        .withEscape(escapeChar)
        .withSkipHeaderRecord(false)
        .withNullString(nullValue)

      val generateHeader = parameters.getOrElse("header", "false").toBoolean
      val header = if (generateHeader) {
        xmlFormat.format(dataFrame.columns.map(_.asInstanceOf[AnyRef]): _*)
      } else {
        "" // There is no need to generate header in this case
      }

      val strRDD = dataFrame.rdd.mapPartitionsWithIndex { case (index, iter) =>
        val xmlFormat = defaultXmlFormat
          .withDelimiter(delimiterChar)
          .withQuote(quoteChar)
          .withEscape(escapeChar)
          .withSkipHeaderRecord(false)
          .withNullString(nullValue)

        new Iterator[String] {
          var firstRow: Boolean = generateHeader

          override def hasNext: Boolean = iter.hasNext || firstRow

          override def next: String = {
            if (iter.nonEmpty) {
              val row = xmlFormat.format(iter.next().toSeq.map(_.asInstanceOf[AnyRef]): _*)
              if (firstRow) {
                firstRow = false
                header + xmlFormat.getRecordSeparator() + row
              } else {
                row
              }
            } else {
              firstRow = false
              header
            }
          }
        }
      }
      compressionCodec match {
        case null => strRDD.saveAsTextFile(path)
        case codec => strRDD.saveAsTextFile(path, codec)
      }
    }
  }
}
