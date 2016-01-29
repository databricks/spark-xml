package com.databricks.spark.xml.parsers

import javax.xml.stream.XMLEventReader
import javax.xml.stream.events._

import com.databricks.spark.xml.XmlOptions

private[xml] object StaxXmlParserUtils {
  /**
   * Skips elements until this meets the given type of a element
   */
  def skipUntil(parser: XMLEventReader, eventType: Int): XMLEvent = {
    var event = parser.nextEvent
    while(parser.hasNext && event.getEventType != eventType) {
      event = parser.nextEvent
    }
    event
  }

  /**
   * Reads the data for all continuous character events within an element.
   */
  def readDataFully(parser: XMLEventReader): String = {
    var event = parser.peek
    var data: String = if (event.isCharacters) "" else null
    while(event.isCharacters) {
      data += event.asCharacters.getData
      parser.nextEvent
      event = parser.peek
    }
    data
  }

  /**
   * Checks if current event points the EndElement.
   */
  def checkEndElement(parser: XMLEventReader, options: XmlOptions): Boolean = {
    val current = parser.peek
    current match {
      case _: EndElement => true
      case _: StartElement => false
      case _: Characters =>
        // When `Characters` is found here, we need to look further to decide
        // if this is really `EndElement` because this can be whitespace between
        // `EndElement` and `StartElement`.
        val next = {
          parser.nextEvent
          parser.peek
        }
        next match {
          case _: EndElement => true
          case _: XMLEvent => false
        }
    }
  }

  /**
   * Produces values map from given attributes.
   */
  def toValuesMap(attributes: Array[Attribute], options: XmlOptions): Map[String, String] = {
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
}
