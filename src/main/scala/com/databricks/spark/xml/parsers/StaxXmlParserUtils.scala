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
import javax.xml.stream.{EventFilter, XMLEventReader, XMLInputFactory, XMLStreamConstants}
import javax.xml.stream.events._

import scala.annotation.tailrec
import scala.collection.JavaConverters._

import com.databricks.spark.xml.XmlOptions

private[xml] object StaxXmlParserUtils {

  private val factory: XMLInputFactory = {
    val factory = XMLInputFactory.newInstance()
    factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false)
    factory.setProperty(XMLInputFactory.IS_COALESCING, true)
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
    while(parser.hasNext && event.getEventType != eventType) {
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
      val attrFields = attributes.map(options.attributePrefix + _.getName.getLocalPart)
      val attrValues = attributes.map(_.getValue)
      val nullSafeValues = {
        if (options.treatEmptyValuesAsNulls) {
          attrValues.map (v => if (v.trim.isEmpty) null else v)
        } else {
          attrValues
        }
      }
      attrFields.zip(nullSafeValues).toMap
    }
  }


  /**
   * Convert the current structure of XML document to a XML string.
   */
  def currentStructureAsString(parser: XMLEventReader): String = {
    // (Hyukjin) I could not find a proper method to produce the current document
    // as a string. For Jackson, there is a method `copyCurrentStructure()`.
    // So, it ended up with manually converting event by event to string.
    def convertChildren(): String = {
      var childrenXmlString = ""
      parser.peek match {
        case _: StartElement =>
          childrenXmlString += currentStructureAsString(parser)
        case c: Characters if c.isWhiteSpace =>
          // There can be a `Characters` event between `StartElement`s.
          // So, we need to check further to decide if this is a data or just
          // a whitespace between them.
          childrenXmlString += c.getData
          parser.next
          parser.peek match {
            case _: StartElement =>
              childrenXmlString += currentStructureAsString(parser)
            case _: XMLEvent =>
              // do nothing
          }
        case c: Characters =>
          childrenXmlString += c.getData
        case _: XMLEvent =>
          // do nothing
      }
      childrenXmlString
    }

    var xmlString = ""
    var shouldStop = false
    while (!shouldStop) {
      parser.nextEvent match {
        case e: StartElement =>
          val attributes = e.getAttributes.asScala.map { a =>
            val att = a.asInstanceOf[Attribute]
            " " + att.getName + "=\"" + att.getValue + "\""
          }.mkString("")
          xmlString += "<" + e.getName + attributes + ">"
          xmlString += convertChildren()
        case e: EndElement =>
          xmlString += "</" + e.getName + ">"
          shouldStop = checkEndElement(parser)
        case _: XMLEvent => // do nothing
      }
    }
    xmlString
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
