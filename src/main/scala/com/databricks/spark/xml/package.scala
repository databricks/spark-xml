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
package com.databricks.spark

import java.io.CharArrayWriter
import javax.xml.stream.XMLOutputFactory

import scala.collection.Map

import com.sun.xml.txw2.output.IndentingXMLStreamWriter
import org.apache.hadoop.io.compress.CompressionCodec

import org.apache.spark.sql.{DataFrame, SQLContext}
import com.databricks.spark.xml.parsers.stax.StaxXmlGenerator
import com.databricks.spark.xml.util.XmlFile

package object xml {
  /**
   * Adds a method, `xmlFile`, to [[SQLContext]] that allows reading XML data.
   */
  implicit class XmlContext(sqlContext: SQLContext) extends Serializable {
    def xmlFile(
                 filePath: String,
                 mode: String = "PERMISSIVE",
                 rowTag: String = XmlFile.DEFAULT_ROW_TAG,
                 samplingRatio: Double = 1.0,
                 excludeAttributeFlag: Boolean = false,
                 treatEmptyValuesAsNulls: Boolean = false,
                 charset: String = XmlFile.DEFAULT_CHARSET.name()): DataFrame = {

      val xmlRelation = XmlRelation(
        () => XmlFile.withCharset(sqlContext.sparkContext, filePath, charset, rowTag),
        location = Some(filePath),
        parseMode = mode,
        samplingRatio = samplingRatio,
        excludeAttributeFlag = excludeAttributeFlag,
        treatEmptyValuesAsNulls = treatEmptyValuesAsNulls)(sqlContext)
      sqlContext.baseRelationToDataFrame(xmlRelation)
    }
  }

  /**
   * Adds a method, `saveAsXmlFile`, to [[DataFrame]] that allows writing XML data.
   */
  implicit class XmlSchemaRDD(dataFrame: DataFrame) {
    // Note that writing a XML file from [[DataFrame]] having a field [[ArrayType]] with
    // its element as [[ArrayType]] would have an additional nested field for the element.
    // For example, the [[DataFrame]] having a field below,
    //
    //   fieldA [[data1, data2]]
    //
    // would produce a XML file below.
    //
    //   <fieldA>
    //       <item>data1</item>
    //       <item>data2</item>
    //   </fieldA>
    //
    // Namely, roundtrip in writing and reading can end up in different schema structure.
    def saveAsXmlFile(path: String, parameters: Map[String, String] = Map(),
                      compressionCodec: Class[_ <: CompressionCodec] = null): Unit = {
      val nullValue = parameters.getOrElse("nullValue", "null")
      val rootTag = parameters.getOrElse("rootTag", XmlFile.DEFAULT_ROOT_TAG)
      val rowTag = parameters.getOrElse("rowTag", XmlFile.DEFAULT_ROW_TAG)
      val startElement = s"<$rootTag>"
      val endElement = s"</$rootTag>"
      val rowSchema = dataFrame.schema
      val indent = XmlFile.DEFAULT_INDENT
      val rowSeparator = XmlFile.DEFAULT_ROW_SEPARATOR

      val xmlRDD = dataFrame.rdd.mapPartitions { iter =>
        val factory = XMLOutputFactory.newInstance()
        val writer = new CharArrayWriter()
        val xmlWriter = factory.createXMLStreamWriter(writer)
        val indentingXmlWriter = new IndentingXMLStreamWriter(xmlWriter)
        indentingXmlWriter.setIndentStep(indent)

        new Iterator[String] {
          var firstRow: Boolean = true
          var lastRow: Boolean = true

          override def hasNext: Boolean = iter.hasNext || firstRow || lastRow

          override def next: String = {
            if (iter.nonEmpty) {
              val xml = {
                StaxXmlGenerator(
                  rowSchema,
                  rowTag,
                  indentingXmlWriter,
                  nullValue)(iter.next())
                writer.toString
              }
              writer.reset()

              // Here it needs to add indentations for the start of each line,
              // in order to insert the start element and end element.
              val indentedXml = indent + xml.replaceAll(rowSeparator, rowSeparator + indent)
              if (firstRow) {
                firstRow = false
                startElement + rowSeparator + indentedXml
              } else {
                indentedXml
              }
            } else {
              indentingXmlWriter.close()
              if (!firstRow) {
                lastRow = false
                endElement
              } else {
                // This means the iterator was initially empty.
                firstRow = false
                lastRow = false
                ""
              }
            }
          }
        }
      }

      compressionCodec match {
        case null => xmlRDD.saveAsTextFile(path)
        case codec => xmlRDD.saveAsTextFile(path, codec)
      }
    }
  }
}
