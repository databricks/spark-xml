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

import java.io.StringWriter
import javax.xml.stream.XMLOutputFactory

import com.sun.xml.txw2.output.IndentingXMLStreamWriter
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.sql.types._

import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import com.databricks.spark.xml.parsers.stax.StaxXmlGenerator
import com.databricks.spark.xml.util.XmlFile

import scala.collection.Map

package object xml {
  /**
   * Adds a method, `xmlFile`, to SQLContext that allows reading XML data.
   */
  implicit class XmlContext(sqlContext: SQLContext) extends Serializable {
    def xmlFile(
                 filePath: String,
                 mode: String = "PERMISSIVE",
                 rowTag: String,
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

  implicit class XmlSchemaRDD(dataFrame: DataFrame) {
    def saveAsXmlFile(path: String, parameters: Map[String, String] = Map(),
                      compressionCodec: Class[_ <: CompressionCodec] = null): Unit = {
      val nullValue = parameters.getOrElse("nullValue", "null")
      val rootTag = parameters.getOrElse("rootTag", XmlFile.DEFAULT_ROOT_TAG)
      val rowTag = parameters.getOrElse("rowTag", XmlFile.DEFAULT_ROW_TAG)
      val startElement = s"<$rootTag>"
      val endElement = s"</$rootTag>"
      val rowSchema = dataFrame.schema
      val indent = XmlFile.DEFAULT_INDENT

      val xmlRDD = dataFrame.rdd.mapPartitions { iter =>
        val factory = XMLOutputFactory.newInstance()

        new Iterator[String] {
          var headingTag: Boolean = true
          var trailingTag: Boolean = true
          override def hasNext: Boolean = iter.hasNext || headingTag || trailingTag

          override def next: String = {
            if (iter.nonEmpty) {
              val writer = new StringWriter()
              val xmlWriter = factory.createXMLStreamWriter(writer)
              val indentingXmlWriter = new IndentingXMLStreamWriter(xmlWriter)
              indentingXmlWriter.setIndentStep(indent)
              val xml = {
                StaxXmlGenerator(
                  rowSchema,
                  rowTag,
                  indentingXmlWriter,
                  nullValue)(iter.next())

                // Manually put indent for the start and end element
                writer.toString.replaceAll("\n", s"\n$indent")
              }

              if (headingTag) {
                headingTag = false
                s"$startElement\n$indent$xml"
              } else {
                s"$indent$xml"
              }
            } else {
              trailingTag = false
              s"$endElement"
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
