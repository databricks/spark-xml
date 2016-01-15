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
package com.databricks.spark

import java.io.CharArrayWriter
import javax.xml.stream.XMLOutputFactory

import scala.collection.Map

import com.sun.xml.internal.txw2.output.IndentingXMLStreamWriter
import org.apache.hadoop.io.compress.CompressionCodec

import org.apache.spark.sql.{DataFrame, SQLContext}
import com.databricks.spark.xml.util.XmlFile
import com.databricks.spark.xml.parsers.StaxXmlGenerator

package object xml {
  private[xml] def compressionCodecClass(className: String): Class[_ <: CompressionCodec] = {
    className match {
      case null => null
      case codec =>
        // scalastyle:off classforname
        Class.forName(codec).asInstanceOf[Class[CompressionCodec]]
        // scalastyle:on classforname
    }
  }

  /**
   * Adds a method, `xmlFile`, to [[SQLContext]] that allows reading XML data.
   */
  implicit class XmlContext(sqlContext: SQLContext) extends Serializable {
    def xmlFile(
                 filePath: String,
                 rowTag: String = XmlOptions.DEFAULT_ROW_TAG,
                 samplingRatio: Double = 1.0,
                 excludeAttribute: Boolean = false,
                 treatEmptyValuesAsNulls: Boolean = false,
                 failFast: Boolean = false,
                 attributePrefix: String = XmlOptions.DEFAULT_ATTRIBUTE_PREFIX,
                 valueTag: String = XmlOptions.DEFAULT_VALUE_TAG,
                 charset: String = XmlOptions.DEFAULT_CHARSET): DataFrame = {

      val parameters = Map(
        "rowTag" -> rowTag,
        "samplingRatio" -> samplingRatio.toString,
        "excludeAttribute" -> excludeAttribute.toString,
        "treatEmptyValuesAsNulls" -> treatEmptyValuesAsNulls.toString,
        "failFast" -> failFast.toString,
        "attributePrefix" -> attributePrefix,
        "valueTag" -> valueTag,
        "charset" -> charset)
      val xmlRelation = XmlRelation(
        () => XmlFile.withCharset(sqlContext.sparkContext, filePath, charset, rowTag),
        location = Some(filePath),
        parameters = parameters.toMap)(sqlContext)
      sqlContext.baseRelationToDataFrame(xmlRelation)
    }
  }

  /**
   * Adds a method, `saveAsXmlFile`, to [[DataFrame]] that allows writing XML data.
   * If compressionCodec is not null the resulting output will be compressed.
   * Note that a codec entry in the parameters map will be ignored.
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
    //   </fieldA>
    //   <fieldA>
    //       <item>data2</item>
    //   </fieldA>
    //
    // Namely, roundtrip in writing and reading can end up in different schema structure.
    def saveAsXmlFile(path: String, parameters: Map[String, String] = Map(),
                      compressionCodec: Class[_ <: CompressionCodec] = null): Unit = {
      val options = XmlOptions.createFromConfigMap(parameters.toMap)
      val startElement = s"<${options.rootTag}>"
      val endElement = s"</${options.rootTag}>"
      val headingContents = options.headingContents
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
                  indentingXmlWriter,
                  options)(iter.next())
                writer.toString
              }
              writer.reset()

              // Here it needs to add indentations for the start of each line,
              // in order to insert the start element and end element.
              val indentedXml = indent + xml.replaceAll(rowSeparator, rowSeparator + indent)
              if (firstRow) {
                firstRow = false
                headingContents + rowSeparator + startElement + rowSeparator + indentedXml
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
