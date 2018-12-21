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

import java.io.StringReader
import javax.xml.stream.events.Attribute
import javax.xml.stream.{XMLInputFactory, XMLStreamConstants}

import scala.collection.JavaConverters._

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import com.databricks.spark.xml.XmlOptions

final class StaxXmlParserUtilsSuite extends FunSuite with BeforeAndAfterAll {

  private val factory = XMLInputFactory.newInstance()
  factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false)
  factory.setProperty(XMLInputFactory.IS_COALESCING, true)

  test("Test if elements are skipped until the given event type") {
    val input = <ROW><id>2</id><name>Sam Mad Dog Smith</name><amount>93</amount></ROW>
    val parser = factory.createXMLEventReader(new StringReader(input.toString))
    val event = StaxXmlParserUtils.skipUntil(parser, XMLStreamConstants.END_DOCUMENT)
    assert(event.isEndDocument)
  }

  test("Check the end of element") {
    val input = <ROW><id>2</id></ROW>
    val parser = factory.createXMLEventReader(new StringReader(input.toString))
    // Skip until </id>
    StaxXmlParserUtils.skipUntil(parser, XMLStreamConstants.END_ELEMENT)
    assert(StaxXmlParserUtils.checkEndElement(parser))
  }

  test("Convert attributes to a map with keys and values") {
    val input = <ROW id="2"></ROW>
    val parser = factory.createXMLEventReader(new StringReader(input.toString))
    val event =
      StaxXmlParserUtils.skipUntil(parser, XMLStreamConstants.START_ELEMENT)
    val attributes =
      event.asStartElement().getAttributes.asScala.map(_.asInstanceOf[Attribute]).toArray
    val valuesMap =
      StaxXmlParserUtils.convertAttributesToValuesMap(attributes, new XmlOptions())
    assert(valuesMap === Map(s"${XmlOptions.DEFAULT_ATTRIBUTE_PREFIX}id" -> "2"))
  }

  test("Convert current structure to string") {
    val input = <ROW><id>2</id><info>
      <name>Sam Mad Dog Smith</name><amount><small>1</small><large>9</large></amount></info></ROW>
    val parser = factory.createXMLEventReader(new StringReader(input.toString))
    // Skip until </id>
    StaxXmlParserUtils.skipUntil(parser, XMLStreamConstants.END_ELEMENT)
    val xmlString = StaxXmlParserUtils.currentStructureAsString(parser)
    val expected = <info>
      <name>Sam Mad Dog Smith</name><amount><small>1</small><large>9</large></amount></info>
    assert(xmlString === expected.toString())
  }

  test("Skip XML children") {
    val input = <ROW><info>
      <name>Sam Mad Dog Smith</name><amount><small>1</small>
        <large>9</large></amount></info><abc>2</abc><test>2</test></ROW>
    val parser = factory.createXMLEventReader(new StringReader(input.toString))
    // We assume here it's reading the value within `id` field.
    StaxXmlParserUtils.skipUntil(parser, XMLStreamConstants.CHARACTERS)
    StaxXmlParserUtils.skipChildren(parser)
    assert(parser.nextEvent().asEndElement().getName.getLocalPart === "info")
    parser.next()
    StaxXmlParserUtils.skipChildren(parser)
    assert(parser.nextEvent().asEndElement().getName.getLocalPart === "abc")
    parser.next()
    StaxXmlParserUtils.skipChildren(parser)
    assert(parser.nextEvent().asEndElement().getName.getLocalPart === "test")
  }
}
