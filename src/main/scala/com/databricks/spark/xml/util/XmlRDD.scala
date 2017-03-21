package com.databricks.spark.xml.util

import java.io.CharArrayWriter
import javax.xml.stream.XMLOutputFactory

import scala.collection.Map

import com.sun.xml.internal.txw2.output.IndentingXMLStreamWriter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import com.databricks.spark.xml.parsers.StaxXmlGenerator
import com.databricks.spark.xml.XmlOptions

object XmlRDD {

  def apply(dataFrame: DataFrame, parameters: Map[String, String]): RDD[String] = {
    val options = XmlOptions(parameters.toMap)
    val rowSchema = dataFrame.schema
    val indent = XmlFile.DEFAULT_INDENT
    val rowSeparator = XmlFile.DEFAULT_ROW_SEPARATOR

    dataFrame.rdd.mapPartitions { iter =>
      val factory = XMLOutputFactory.newInstance()
      val writer = new CharArrayWriter()
      val xmlWriter = factory.createXMLStreamWriter(writer)
      val indentingXmlWriter = new IndentingXMLStreamWriter(xmlWriter)
      indentingXmlWriter.setIndentStep(indent)

      indentingXmlWriter.writeStartElement(options.rootTag)
      for ((name, value) <- options.rootAttributes) {
        indentingXmlWriter.writeAttribute(name, value)
      }
      val startElement = writer.toString
      writer.reset()

      new Iterator[String] {
        var firstRow: Boolean = true
        var lastRow: Boolean = iter.nonEmpty

        override def hasNext: Boolean = iter.hasNext || lastRow

        override def next: String = {
          if (iter.nonEmpty) {
            if (firstRow) {
              firstRow = false
              startElement + ">"
            } else {
              val xml = {
                StaxXmlGenerator(
                  rowSchema,
                  indentingXmlWriter,
                  options)(iter.next())
                writer.toString
              }
              writer.reset()
              xml.stripPrefix(">").stripPrefix(rowSeparator)
            }
          } else {
            indentingXmlWriter.writeEndDocument()
            val endElement = writer.toString
            indentingXmlWriter.close()
            lastRow = false
            endElement
          }
        }
      }
    }
  }
}
