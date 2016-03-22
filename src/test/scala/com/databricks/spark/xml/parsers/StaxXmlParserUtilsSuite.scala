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

    assert(StaxXmlParserUtils.checkEndElement(parser))
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

    assert(valuesMap === Map(s"${XmlOptions.DEFAULT_ATTRIBUTE_PREFIX}id" -> "2"))
  }

  test("Convert current structure to string") {
    val input = <ROW><id>2</id><info>
      <name>Sam Mad Dog Smith</name><amount>93</amount></info></ROW>
    val reader = new ByteArrayInputStream(input.toString().getBytes)
    val parser = factory.createXMLEventReader(reader)
    // Skip until </id>
    StaxXmlParserUtils.skipUntil(parser, XMLStreamConstants.END_ELEMENT)
    val xmlString = StaxXmlParserUtils.currentStructureAsString(parser)

    val expected = <info>
      <name>Sam Mad Dog Smith</name><amount>93</amount></info>
    assert(xmlString === expected.toString())
  }
}
