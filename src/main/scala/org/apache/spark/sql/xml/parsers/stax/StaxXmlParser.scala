/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.xml.parsers.stax

import javax.xml.stream.events.{Attribute, XMLEvent}
import javax.xml.stream.{XMLEventReader, XMLInputFactory, XMLStreamException, XMLStreamReader}

import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.xml.parsers.io.BaseRDDInputStream
import org.apache.spark.sql.xml.util.TypeCast._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * This defines the possible types for XML.
 */
private[sql] object XmlDataTypes {
  val FAIL: Int = -1
  val NULL: Int = 1
  val BOOLEAN: Int = 2
  val LONG: Int = 3
  val DOUBLE: Int = 4
  val STRING: Int = 5
  val OBJECT: Int = 6
  val ARRAY: Int = 7
}


private[sql] class StaxXmlParser(parser: XMLEventReader) {
  import org.apache.spark.sql.xml.parsers.stax.XmlDataTypes._

  private var startField: Option[String] = None
  private var currentField: Option[String] = None
  var eventsInFragment: ArrayBuffer[XMLEvent] = ArrayBuffer.empty[XMLEvent]

  /**
   * In this parser, this method should be called for `next()` and `hasNext()`.
   * The fragment means the object started with the root and ended with the root.
   * If not given, then it starts with the first met element.
   */

  // TODO: Polish here
  def readFragment(): Boolean = {

    if (startField.isEmpty) {
      // By default we skip the root element in the XML file
      // Find the first event
      var isFirstFound = false
      var lastEvent: Option[XMLEvent] = None
      while (parser.hasNext && !isFirstFound) {
        val event = parser.nextEvent
        if (event.isStartElement) {
          lastEvent = seekStartEvent(parser)
          isFirstFound = true
        }
      }

      // TODO: Polish here
      val field = lastEvent.map(_.asStartElement.getName.getLocalPart)
      val attrIter = lastEvent.map(_.asStartElement.getAttributes.asScala.map(_.asInstanceOf[XMLEvent]))
      lastEvent.map(eventsInFragment += _)
      eventsInFragment = eventsInFragment ++ attrIter.getOrElse(ArrayBuffer.empty[XMLEvent])
      startField = field
      field.exists(readFragment)
    } else {
      val lastEvent = seekStartEvent(parser)
      val field = lastEvent.map(_.asStartElement.getName.getLocalPart)
      val attrIter = lastEvent.map(_.asStartElement.getAttributes.asScala.map(_.asInstanceOf[XMLEvent]))
      lastEvent.map(eventsInFragment += _)
      eventsInFragment = eventsInFragment ++ attrIter.getOrElse(ArrayBuffer.empty[XMLEvent])
      field.exists(readFragment)
    }
  }

  private def seekStartEvent(parser: XMLEventReader): Option[XMLEvent] = {
    var event = parser.nextEvent
    var found = false
    while (parser.hasNext && !found) {
      event = parser.nextEvent
      if (event.isStartElement) {
        found = true
      }
    }
    if (found) {
      Some(event)
    } else {
      None
    }
  }

  def readFragment(root: String): Boolean = {
    var field = root
    var isFirstFound = false

    // Find the first event
    val currentEvent = getCurrentEvent
    if (currentEvent.isStartElement) {
      if (field == currentEvent.asStartElement.getName.getLocalPart) {
        isFirstFound = true
      }
    }

    while (parser.hasNext && !isFirstFound) {
      val event = parser.nextEvent
      if (event.isStartElement) {
        val startEvent = event.asStartElement
        if (field == startEvent.getName.getLocalPart) {
          isFirstFound = true
          val attrIter = event.asStartElement.getAttributes.asScala.map(_.asInstanceOf[XMLEvent])
          eventsInFragment += event
          eventsInFragment = eventsInFragment ++ attrIter
        }
      }
    }

    // Find the last event
    var isLastFound = false
    while (parser.hasNext && !isLastFound) {
      val event = parser.nextEvent
      val isWhiteSpace = event.isCharacters && StringUtils.isBlank(event.asCharacters.getData)
      if (!isWhiteSpace) {
        eventsInFragment += event
      }

      if (event.isStartElement) {
        val attrIter = event.asStartElement.getAttributes.asScala.map(_.asInstanceOf[XMLEvent])
        eventsInFragment = eventsInFragment ++ attrIter
      } else if (event.isEndElement) {
        val lastEvent = event.asEndElement
        if (field == lastEvent.getName.getLocalPart) {
          isLastFound = true
        }
      }
    }

    if (eventsInFragment.isEmpty){
      false
    } else {
      currentField = Some(eventsInFragment.last.asEndElement.getName.getLocalPart)
      true
    }
  }


  def skipEndElementUntil(field: String): Boolean = {
    var shouldSkip = true
    while (hasNext && shouldSkip) {
      val event = eventsInFragment.head
      if (event.isEndElement && currentField.contains(field)) {
        shouldSkip = false
      } else if (event.isEndElement) {
        nextEvent
        shouldSkip = true
      } else {
        shouldSkip = false
      }
    }

    !shouldSkip
  }

  def hasNext = eventsInFragment.nonEmpty

  def getCurrentEvent = eventsInFragment.head

  def nextEvent: XMLEvent = {
    val event = eventsInFragment.remove(0)
    if (event.isStartElement) {
      currentField = Some(event.asStartElement.getName.getLocalPart)
    } else if (event.isEndElement) {
      currentField = Some(event.asEndElement.getName.getLocalPart)
    }
    event
  }

  def clear(): Unit = {
    eventsInFragment.clear()
  }

  def inferNonNestedType(data: String): Int = {
    if (isLong(data)) {
      LONG
    } else if (isDouble(data)) {
      DOUBLE
    } else if (isBoolean(data)) {
      BOOLEAN
    } else {
      STRING
    }
  }

  private def inferNestedType(field: String): Int = {
    val isObject = eventsInFragment.filter {
      case event => event.isStartElement
    }.map {
      case startEvent => startEvent.asStartElement.getName.getLocalPart
    }.contains(field)

    if (isObject) {
      OBJECT
    } else {
      ARRAY
    }
  }

  /**
   * This infers the type of data.
   */
  def inferDataType: Int = {
    val currentEvent = eventsInFragment.head
    if (currentEvent.isCharacters) {
      inferNonNestedType(currentEvent.asCharacters.getData)
    } else if (currentEvent.isAttribute) {
      inferNonNestedType(currentEvent.asInstanceOf[Attribute].getValue)
    } else if (currentEvent.isStartElement && eventsInFragment(1).isEndElement) {
      NULL
    } else if (currentEvent.isStartElement && eventsInFragment.exists(_.isStartElement)) {
      inferNestedType(currentEvent.asStartElement.getName.getLocalPart)
    } else {
      FAIL
    }
  }
}


/**
 * Wraps parser to iteratoration process.
 */
private[sql] object StaxXmlRDD {

  def apply(xml: RDD[String], schema: StructType)
           (sqlContext: SQLContext): RDD[Row] = {
    val factory = XMLInputFactory.newInstance()
    val parser = new StaxXmlParser(factory.createXMLEventReader(new BaseRDDInputStream(xml)))
    val stream = this(parser, schema)
    sqlContext.sparkContext.parallelize[Row](stream)
  }

  private def apply(parser: StaxXmlParser, schema: StructType): Stream[Row] = {

    new Iterator[Row] {
      var record: Row = _

      override def hasNext: Boolean = {
        try {
          val maybeRecord = startConvertObject(parser, schema)
          if (maybeRecord.isDefined) {
            record = Row.fromSeq(maybeRecord.get.toSeq(schema))
            true
          } else{
            false
          }
        } catch {
          // TODO: Permissive mode here
          case e: XMLStreamException =>
            record = failedRecord(e)
            false
        }
      }

      private def failedRecord(e: XMLStreamException): Row = {
        // TODO: We should fill the elements more.
        Row.fromSeq(Array[Any](schema.length))
      }

      override def next: Row = {
        record
      }
    }.toStream
  }

  /**
   * Parse the current token (and related children) according to a desired schema
   */
  private def startConvertObject(parser: StaxXmlParser, schema: StructType): Option[InternalRow] = {
    if (parser.readFragment()) {
      val field = parser.nextEvent.asStartElement.getName.getLocalPart
      Some(convertObject(parser, schema, field))
    } else {
      None
    }
  }

  // TODO: get rid of current event and polish here
  private[sql] def convertField(parser: StaxXmlParser,
                                schema: DataType,
                                parentField: String): Any = {
    schema match {
      case LongType =>
        val event = parser.nextEvent
        if (event.isCharacters) {
          signSafeToLong(event.asCharacters.getData)
        } else {
          null
        }

      case DoubleType =>
        val event = parser.nextEvent
        if (event.isCharacters) {
          signSafeToDouble(event.asCharacters.getData)
        } else {
          null
        }

      case BooleanType =>
        val event = parser.nextEvent
        if (event.isCharacters) {
          event.asCharacters.getData.toBoolean
        } else {
          null
        }

      case StringType =>
        val event = parser.nextEvent
        if (event.isCharacters) {
          UTF8String.fromString(event.asCharacters.getData)
        } else {
          null
        }

      case NullType =>
        parser.nextEvent
        null

      case ArrayType(st, _) =>
        convertPartialArray(parser, st, parentField)

      case st: StructType =>
        convertObject(parser, st, parentField)

      case (udt: UserDefinedType[_]) =>
        convertField(parser, udt.sqlType, parentField)

      case dataType =>
        sys.error(s"Failed to parse a value for data type $dataType.")
    }
  }

  /**
   * Parse an object from the token stream into a new Row representing the schema.
   *
   * Fields in the json that are not defined in the requested schema will be dropped.
   */
  private def convertObject(parser: StaxXmlParser,
                            schema: StructType,
                            parentField: String): InternalRow = {
    var row = new GenericMutableRow(schema.length)
    while (parser.skipEndElementUntil(parentField)) {
      val event = parser.nextEvent
      if (event.isAttribute) {
        val field = event.asInstanceOf[Attribute].getName.getLocalPart
        schema.getFieldIndex(field) match {
          case Some(index) =>
            // TODO: Currently we only hanlde attribute as string but
            // all the structure might have to be changed for this.
            row(index) = UTF8String.fromString(event.asInstanceOf[Attribute].getValue)
        }
      } else {
        // TODO: We might have to add a case when field is null
        // although this case is impossible.
        val field = event.asStartElement.getName.getLocalPart
        schema.getFieldIndex(field) match {
          case Some(index) =>
            // For XML, it can contains the same keys. So we need to manually merge them to an array.
            // TODO: This routine  is hacky and should go out of this.
            val dataType = schema(index).dataType
            dataType match {
              case ArrayType(st, _) =>
                val values = row.get(index, dataType)
                val newValues = convertField(parser, dataType, parentField).asInstanceOf[Array[Any]]
                row(index) = new GenericArrayData(values +: newValues)
              case _ =>
                row(index) = convertField(parser, dataType, parentField)
            }
        }
      }
    }
    row
  }

  /**
   * Parse an object as a array
   */
  private def convertPartialArray(parser: StaxXmlParser,
                                  elementType: DataType,
                                  parentField: String): Array[Any] = {
    val values = ArrayBuffer.empty[Any]
    values += convertField(parser, elementType, parentField)
    values.toArray
  }
}
