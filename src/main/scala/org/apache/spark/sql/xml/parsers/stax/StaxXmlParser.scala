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

import java.io.ByteArrayInputStream
import javax.xml.stream.events.{Attribute, XMLEvent}
import javax.xml.stream.{XMLEventReader, XMLInputFactory}

import com.sun.xml.internal.stream.events.CharacterEvent
import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.xml.util.TypeCast._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

private[sql] class StaxXmlParser(parser: XMLEventReader) {
  import org.apache.spark.sql.xml.parsers.stax.StaxXmlParser._

  private var startField: Option[String] = None
  private var currentField: Option[String] = None
  var eventsInFragment: ArrayBuffer[XMLEvent] = ArrayBuffer.empty[XMLEvent]

  /**
   * Read all the event to infer datatypes.
   */

  def readAllEventsInFragment: Boolean = {
    var isLastCharators = false
    while (parser.hasNext) {
      val event = parser.nextEvent
      if (event.isCharacters && !StringUtils.isBlank(event.asCharacters.getData)) {

        // We need to concatenate values if character token is found again.
        // Java StAX XML produces character event sequentially sometimes.
        // TODO: Check why it is.
        if(eventsInFragment.last.isCharacters){
          val previous = eventsInFragment.last.asCharacters.getData
          val current = event.asCharacters.getData
          eventsInFragment.last.asInstanceOf[CharacterEvent].setData(previous + current)
        } else {
          eventsInFragment += event
        }
      } else if (event.isStartElement) {
        val attrIter = event.asStartElement.getAttributes.asScala.map(_.asInstanceOf[XMLEvent])
        eventsInFragment += event
        eventsInFragment = eventsInFragment ++ attrIter
      } else if (event.isEndElement) {

        // We keep this to give null in case it only has the start and end tag without value.
        eventsInFragment += event
      }
    }
    eventsInFragment.nonEmpty
  }


  def skipEndElementUntil(field: String): Boolean = {
    var shouldSkip = true
    while (eventsInFragment.nonEmpty && shouldSkip) {
      val event = eventsInFragment.head
      if (event.isEndElement) {
        nextEvent
        shouldSkip = true
      } else {
        shouldSkip = false
      }
    }

    !shouldSkip
  }

  def nextEvent: XMLEvent = {
    eventsInFragment.remove(0)
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
private[sql] object StaxXmlParser {

  /**
   * This defines the possible types for XML.
   */

  val FAIL: Int = -1
  val NULL: Int = 1
  val BOOLEAN: Int = 2
  val LONG: Int = 3
  val DOUBLE: Int = 4
  val STRING: Int = 5
  val OBJECT: Int = 6
  val ARRAY: Int = 7

  def apply(xml: RDD[String], schema: StructType, rootTag: String)
           (sqlContext: SQLContext): RDD[Row] = {
    xml.mapPartitions { iter =>
      iter.flatMap { xml =>
        val factory = XMLInputFactory.newInstance()
        val parser = new StaxXmlParser(factory.createXMLEventReader(new ByteArrayInputStream(xml.getBytes)))
        // Skip the first event
        startConvertObject(parser, schema, rootTag)
      }
    }
  }

  /**
   * Parse the current token (and related children) according to a desired schema
   */
  private def startConvertObject(parser: StaxXmlParser, schema: StructType, rootTag: String): Option[Row] = {
    if (parser.readAllEventsInFragment) {
      parser.nextEvent
      val record = convertObject(parser, schema, rootTag)
      Some(Row.fromSeq(record.toSeq(schema)))
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
