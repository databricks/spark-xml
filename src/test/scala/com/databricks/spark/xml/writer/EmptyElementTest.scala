package com.databricks.spark.xml.writer

import com.sun.xml.internal.txw2.output.IndentingXMLStreamWriter
import javax.xml.stream.XMLOutputFactory


object EmptyElementTest {
  def main(args: Array[String]): Unit = {
    val factory = XMLOutputFactory.newInstance()
    val writer = System.out
    val xmlWriter = factory.createXMLStreamWriter(writer)
    val indentingXmlWriter = new IndentingXMLStreamWriter(xmlWriter)
    indentingXmlWriter.writeStartDocument("UTF-8", "1.0")
    indentingXmlWriter.setIndentStep("   ")
    indentingXmlWriter.writeStartElement("Row")
    indentingXmlWriter.writeEmptyElement("selfcloseTag")
    indentingXmlWriter.writeAttribute("url", "www.ahuoo.com")
    indentingXmlWriter.writeAttribute("src", "www.ahuoo.com")
    // indentingXmlWriter.writeStartElement("Row")
    indentingXmlWriter.writeEndElement()
    indentingXmlWriter.writeEndDocument()
    indentingXmlWriter.flush()
  }
}
