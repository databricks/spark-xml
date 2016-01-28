package com.databricks.spark.xml.parsers

import javax.xml.stream.XMLEventReader
import javax.xml.stream.events.XMLEvent

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
   * Read the data for all continuous character events within an element.
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
}
