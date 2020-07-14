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

  @scala.annotation.tailrec
  def structureAsString(path: XmlPath, parser: TrackingXmlEventReader, accumulatedStrings: Seq[String] = Seq.empty)(implicit options: XmlOptions): String = {
    def makeFinalString() = if (options.ignoreSurroundingSpaces) {
      accumulatedStrings.mkString("").trim()
    } else {
      accumulatedStrings.mkString("")
    }

    parser.nextEvent() match {
      case startElement: StartElement =>
        val attributes = convertAttributesToValuesMap(startElement.getAttributes.asScala.map(_.asInstanceOf[Attribute]).toArray, options)
        structureAsString(path, parser,
          accumulatedStrings :+ s"<${startElement.getName.getLocalPart}${attributes.map { case (key, value) => s""" "$key"="$value"""" }.mkString("")}>")
      case endElement: EndElement =>
        val endElementName = endElement.getName.getLocalPart
        val endElementXml = s"</$endElementName>"
        val startElementXmlPrefix = s"<$endElementName"
        if (parser.currentPath.child(endElementName) == path) {
          makeFinalString()
        } else {
          // Check if this should be a self closing element
          val updatedAccumulatedStrings = accumulatedStrings.lastOption match {
            case Some(lastElementString) if lastElementString.takeWhile(string => string != ' ' && string != '>') == startElementXmlPrefix =>
              accumulatedStrings.dropRight(1) :+ lastElementString.dropRight(1) + "/>"
            case _ =>
              accumulatedStrings :+ endElementXml
          }

          structureAsString(path, parser, updatedAccumulatedStrings)
        }
      case _: EndDocument => makeFinalString()
      case other: XMLEvent =>
        structureAsString(path, parser, accumulatedStrings :+ other.toString)
    }
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
