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
package com.databricks.spark.xml.util

import java.io.CharArrayWriter
import java.nio.charset.Charset
import javax.xml.stream.XMLOutputFactory

import scala.collection.Map

import com.databricks.spark.xml.parsers.StaxXmlGenerator
import com.sun.xml.internal.txw2.output.IndentingXMLStreamWriter
import org.apache.hadoop.io.{Text, LongWritable}

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import com.databricks.spark.xml.{XmlOptions, XmlInputFormat}

private[xml] object XmlFile {
  val DEFAULT_INDENT = "    "
  val DEFAULT_ROW_SEPARATOR = "\n"

  def withCharset(
      context: SparkContext,
      location: String,
      charset: String,
      rowTag: String): RDD[String] = {
    context.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, s"<$rowTag>")
    context.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, s"</$rowTag>")
    context.hadoopConfiguration.set(XmlInputFormat.ENCODING_KEY, charset)
    if (Charset.forName(charset) == Charset.forName(XmlOptions.DEFAULT_CHARSET)) {
      context.newAPIHadoopFile(location,
        classOf[XmlInputFormat],
        classOf[LongWritable],
        classOf[Text]).map(pair => new String(pair._2.getBytes, 0, pair._2.getLength))
    } else {
      context.newAPIHadoopFile(location,
        classOf[XmlInputFormat],
        classOf[LongWritable],
        classOf[Text]).map(pair => new String(pair._2.getBytes, 0, pair._2.getLength, charset))
    }
  }

  /**
   * Note that writing a XML file from [[DataFrame]] having a field
   * [[org.apache.spark.sql.types.ArrayType]] with its element as nested array would have
   * an additional nested field for the element. For example, the [[DataFrame]] having
   * a field below,
   *
   *   fieldA Array(Array(data1, data2))
   *
   * would produce a XML file below.
   *
   * <fieldA>
   *     <item>data1</item>
   * </fieldA>
   * <fieldA>
   *     <item>data2</item>
   * </fieldA>
   *
   * Namely, roundtrip in writing and reading can end up in different schema structure.
   */
  def saveAsXmlFile(
      dataFrame: DataFrame,
      path: String,
      parameters: Map[String, String] = Map()): Unit = {
    val options = XmlOptions(parameters.toMap)
    val codecClass = CompressionCodecs.getCodecClass(options.codec)
    val startElement = s"<${options.rootTag}>"
    val endElement = s"</${options.rootTag}>"
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
            if (firstRow) {
              indentingXmlWriter.writeStartElement(options.rootTag)
              firstRow = false
            }
            val xml = {
              StaxXmlGenerator(
                rowSchema,
                indentingXmlWriter,
                options)(iter.next())
              writer.toString
            }
            writer.reset()
            xml
          } else {
            if (!firstRow) {
              lastRow = false
              indentingXmlWriter.writeEndElement()
              indentingXmlWriter.close()
              writer.toString
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

    codecClass match {
      case null => xmlRDD.saveAsTextFile(path)
      case codec => xmlRDD.saveAsTextFile(path, codec)
    }
  }
}
