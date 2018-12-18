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
  def checkEndElement(parser: XMLEventReader): Boolean = {
    parser.peek match {
      case _: EndElement => true
      case _: EndDocument => true
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
        case e: StartElement =>
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
        case e: XMLEvent =>
          shouldStop = shouldStop && parser.hasNext
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
        case e: StartElement =>
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
        case _: XMLEvent =>
          shouldStop = shouldStop && parser.hasNext
      }
    }
  }
}
