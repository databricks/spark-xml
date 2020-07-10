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

import com.databricks.spark.xml.parsers.StaxXmlParser.TrackingXmlEventReader
import com.databricks.spark.xml.{ XmlOptions, XmlPath }
import javax.xml.stream.events._
import javax.xml.stream.{ EventFilter, XMLEventReader, XMLInputFactory, XMLStreamConstants }

import scala.annotation.tailrec
import scala.collection.JavaConverters._

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
  @tailrec
  def skipUntil(parser: XMLEventReader, eventType: Int): XMLEvent = {
    val nextEvent = parser.nextEvent()
    if (nextEvent.getEventType == eventType) {
      nextEvent
    } else {
      skipUntil(parser, eventType)
    }
  }

  /**
   * Checks if current event points the EndElement.
   */
  @tailrec
  def checkEndElement(parser: XMLEventReader): Boolean = {
    parser.peek match {
      case _: EndElement | _: EndDocument => true
      case c: Characters if c.isWhiteSpace =>
        parser.nextEvent()
        checkEndElement(parser)
      case _ => false
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
          attrValues.map(v => if (v.trim.isEmpty) null else v)
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
          xmlString += "<" + e.getName + ">"
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
   * Skip the children of XML element.
   */
  @tailrec
  def skipChildren(path: XmlPath, parser: TrackingXmlEventReader): EndElement = {
    val nextEvent = parser.nextEvent()
    nextEvent match {
      case endElement: EndElement if path.isChildOf(parser.currentPath)
        && path.leafName == endElement.getName.getLocalPart => endElement
      case _ => skipChildren(path, parser)
    }
  }
}
