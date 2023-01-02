/*
 * Copyright 2019 Databricks
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
import javax.xml.namespace.QName
import javax.xml.stream.{EventFilter, XMLEventReader, XMLInputFactory, XMLStreamConstants}
import javax.xml.stream.events._

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

import com.databricks.spark.xml.XmlOptions

private[xml] object StaxXmlParserUtils {

  private[xml] val factory: XMLInputFactory = {
    val factory = XMLInputFactory.newInstance()
    factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false)
    factory.setProperty(XMLInputFactory.IS_COALESCING, true)
    factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false)
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, false)
    factory
  }

  def filteredReader(xml: String): XMLEventReader = {
    val filter = new EventFilter {
      override def accept(event: XMLEvent): Boolean =
        // Ignore comments and processing instructions
        event.getEventType match {
          case XMLStreamConstants.COMMENT | XMLStreamConstants.PROCESSING_INSTRUCTION => false
          case _ => true
        }
    }
    // It does not have to skip for white space, since `XmlInputFormat`
    // always finds the root tag without a heading space.
    val eventReader = factory.createXMLEventReader(new StringReader(xml))
    factory.createFilteredReader(eventReader, filter)
  }

  def gatherRootAttributes(parser: XMLEventReader): Array[Attribute] = {
    val rootEvent =
      StaxXmlParserUtils.skipUntil(parser, XMLStreamConstants.START_ELEMENT)
    rootEvent.asStartElement.getAttributes.asScala.map(_.asInstanceOf[Attribute]).toArray
  }

  /**
   * Skips elements until this meets the given type of a element
   */
  def skipUntil(parser: XMLEventReader, eventType: Int): XMLEvent = {
    var event = parser.peek
    while (parser.hasNext && event.getEventType != eventType) {
      event = parser.nextEvent
    }
    event
  }

  /**
   * Checks if current event points the EndElement.
   */
  @tailrec
  def checkEndElement(parser: XMLEventReader): Boolean = {
    parser.peek match {
      case _: EndElement | _: EndDocument => true
      case _: StartElement => false
      case _ =>
        // When other events are found here rather than `EndElement` or `StartElement`
        // , we need to look further to decide if this is the end because this can be
        // whitespace between `EndElement` and `StartElement`.
        parser.nextEvent
        checkEndElement(parser)
    }
  }

  /**
   * Produces values map from given attributes.
   */
  def convertAttributesToValuesMap(
      attributes: Array[Attribute],
      options: XmlOptions): Map[String, String] = {
    if (options.excludeAttributeFlag) {
      Map.empty[String, String]
    } else {
      attributes.map { attr =>
        val key = options.attributePrefix + getName(attr.getName, options)
        val value = attr.getValue match {
          case v if options.treatEmptyValuesAsNulls && v.trim.isEmpty => null
          case v => v
        }
        key -> value
      }.toMap
    }
  }

  /**
   * Gets the local part of an XML name, optionally without namespace.
   */
  def getName(name: QName, options: XmlOptions): String = {
    val localPart = name.getLocalPart
    // Ignore namespace prefix up to last : if configured
     if (options.ignoreNamespace) {
      localPart.split(":").last
    } else {
      localPart
    }
  }

  /**
   * Convert the current structure of XML document to a XML string.
   */
  def currentStructureAsString(parser: XMLEventReader): String = {
    val xmlString = new StringBuilder()
    var indent = 0
    do {
      parser.nextEvent match {
        case e: StartElement =>
          xmlString.append('<').append(e.getName)
          e.getAttributes.asScala.foreach { a =>
            val att = a.asInstanceOf[Attribute]
            xmlString.append(' ').append(att.getName).append("=\"").
              append(att.getValue).append('"')
          }
          xmlString.append('>')
          indent += 1
        case e: EndElement =>
          xmlString.append("</").append(e.getName).append('>')
          indent -= 1
        case c: Characters =>
          xmlString.append(c.getData)
        case _: XMLEvent => // do nothing
      }
    } while (parser.peek() match {
      case _: EndElement =>
        // until the unclosed end element for the whole parent is found
        indent > 0
      case _ => true
    })
    xmlString.toString()
  }

  /**
   * Skip the children of the current XML element.
   */
  def skipChildren(parser: XMLEventReader): Unit = {
    var shouldStop = checkEndElement(parser)
    while (!shouldStop) {
      parser.nextEvent match {
        case _: StartElement =>
          val e = parser.peek
          if (e.isCharacters && e.asCharacters.isWhiteSpace) {
            // There can be a `Characters` event between `StartElement`s.
            // So, we need to check further to decide if this is a data or just
            // a whitespace between them.
            parser.next
          }
          if (parser.peek.isStartElement) {
            skipChildren(parser)
          }
        case _: EndElement =>
          shouldStop = checkEndElement(parser)
        case _: XMLEvent => // do nothing
      }
    }
  }
}
