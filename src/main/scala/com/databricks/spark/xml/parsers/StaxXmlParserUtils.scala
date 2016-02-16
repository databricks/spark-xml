package com.databricks.spark.xml.parsers

import javax.xml.stream.XMLEventReader
import javax.xml.stream.events._

import com.databricks.spark.xml.XmlOptions

private[xml] object StaxXmlParserUtils {
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
  def checkEndElement(parser: XMLEventReader, options: XmlOptions): Boolean = {
    parser.peek match {
      case _: EndElement => true
      case _: StartElement => false
      case _: XMLEvent =>
        // When other events are found here rather than `EndElement` or `StartElement`
        // , we need to look further to decide if this is the end because this can be
        // whitespace between `EndElement` and `StartElement`.
        parser.nextEvent
        checkEndElement(parser, options)
    }
  }

  /**
   * Produces values map from given attributes.
   */
  def convertAttributesToValuesMap(attributes: Array[Attribute],
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
}
