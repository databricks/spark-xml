package com.databricks.spark.xml.parsers

import java.io.ByteArrayInputStream
import javax.xml.stream.events.Attribute
import javax.xml.stream.{XMLStreamConstants, XMLInputFactory}

import scala.collection.JavaConversions._

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import com.databricks.spark.xml.XmlOptions

class StaxXmlParserUtilsSuite extends FunSuite with BeforeAndAfterAll {
  val factory = XMLInputFactory.newInstance()
  factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false)
  factory.setProperty(XMLInputFactory.IS_COALESCING, true)

  test("Test if elements are skipped until the given event type") {
    val input = <ROW><id>2</id><name>Sam Mad Dog Smith</name><amount>93</amount></ROW>
    val reader = new ByteArrayInputStream(input.toString().getBytes)
    val parser = factory.createXMLEventReader(reader)
    val event = StaxXmlParserUtils.skipUntil(parser, XMLStreamConstants.END_DOCUMENT)

    assert(event.isEndDocument)
  }

  test("Check the end of element") {
    val input = <ROW><id>2</id></ROW>
    val reader = new ByteArrayInputStream(input.toString().getBytes)
    val parser = factory.createXMLEventReader(reader)
    // Skip until </id>
    StaxXmlParserUtils.skipUntil(parser, XMLStreamConstants.END_ELEMENT)

    assert(StaxXmlParserUtils.checkEndElement(parser, new XmlOptions(Map())))
  }

  test("Convert attributes to a map with keys and values") {
    val input = <ROW id="2"></ROW>
    val reader = new ByteArrayInputStream(input.toString().getBytes)
    val parser = factory.createXMLEventReader(reader)
    val event =
      StaxXmlParserUtils.skipUntil(parser, XMLStreamConstants.START_ELEMENT)
    val attributes =
      event.asStartElement().getAttributes.map(_.asInstanceOf[Attribute]).toArray
    val valuesMap =
      StaxXmlParserUtils.convertAttributesToValuesMap(attributes, new XmlOptions(Map()))

    assert(valuesMap === Map("@id" -> "2"))
  }
}
