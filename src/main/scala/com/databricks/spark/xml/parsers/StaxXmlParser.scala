/*
 * Copyright 2014 Databricks
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

import java.io.ByteArrayInputStream
import javax.xml.stream.events.{Attribute, XMLEvent}
import javax.xml.stream.events._
import javax.xml.stream.{XMLStreamException, XMLStreamConstants, XMLEventReader, XMLInputFactory}

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

import org.slf4j.LoggerFactory

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import com.databricks.spark.xml.util.TypeCast._
import com.databricks.spark.xml.util.XmlFile

/**
 *  Configuration used during parsing.
 */
private[xml] case class StaxConfiguration(
  samplingRatio: Double = 1.0,
  excludeAttributeFlag: Boolean = false,
  treatEmptyValuesAsNulls: Boolean = false,
  failFastFlag: Boolean = false,
  attributePrefix: String = XmlFile.DEFAULT_ATTRIBUTE_PREFIX,
  valueTag: String = XmlFile.DEFAULT_VALUE_TAG
)

/**
 * Wraps parser to iteration process.
 */
private[xml] object StaxXmlParser {
  private val logger = LoggerFactory.getLogger(StaxXmlParser.getClass)

  def parse(xml: RDD[String],
            schema: StructType,
            conf: StaxConfiguration): RDD[Row] = {
    val failFast = conf.failFastFlag
    xml.mapPartitions { iter =>
      iter.flatMap { xml =>
        // It does not have to skip for white space, since `XmlInputFormat`
        // always finds the root tag without a heading space.
        val factory = XMLInputFactory.newInstance()
        val reader = new ByteArrayInputStream(xml.getBytes)
        val parser = factory.createXMLEventReader(reader)
        try {
          val rootAttributes = {
            val rootEvent = skipUntil(parser, XMLStreamConstants.START_ELEMENT)
            rootEvent.asStartElement.getAttributes
              .map(_.asInstanceOf[Attribute]).toArray
          }
          Some(convertObject(parser, schema, conf, rootAttributes))
        } catch {
          case _: java.lang.NumberFormatException if !failFast =>
            logger.warn("Number format exception. " +
              s"Dropping malformed line: ${xml.replaceAll("\n", "")}")
            None
          case _: java.text.ParseException | _: IllegalArgumentException  if !failFast =>
            logger.warn("Parse exception. " +
              s"Dropping malformed line: ${xml.replaceAll("\n", "")}")
            None
          case _: XMLStreamException if failFast =>
            throw new RuntimeException(s"Malformed row (failing fast): ${xml.replaceAll("\n", "")}")
          case _: XMLStreamException if !failFast =>
            logger.warn(s"Dropping malformed row: ${xml.replaceAll("\n", "")}")
            None
        }
      }
    }
  }

  /**
   * Skips elements until this meets the given type of a element
   */
  def skipUntil(parser: XMLEventReader, eventType: Int): XMLEvent = {
    var event = parser.nextEvent()
    while(parser.hasNext && event.getEventType != eventType) {
      event = parser.nextEvent()
    }
    event
  }

  /**
   * Check if current event points the EndElement.
   */
  def checkEndElement(parser: XMLEventReader, conf: StaxConfiguration): Boolean = {
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
          case _: StartElement => false
          case e: XMLEvent =>
            sys.error(s"Failed to parse data with unexpected event ${e.toString}")
        }
    }
  }

  /**
   * Parse the current token (and related children) according to a desired schema
   */
  private[xml] def convertField(parser: XMLEventReader,
                                 dataType: DataType,
                                 conf: StaxConfiguration): Any = {
    def convertComplicatedType: DataType => Any = {
      case dt: StructType => convertObject(parser, dt, conf)
      case MapType(StringType, vt, _) => convertMap(parser, vt, conf)
      case ArrayType(st, _) => convertField(parser, st, conf)
      case udt: UserDefinedType[_] => convertField(parser, udt.sqlType, conf)
    }

    val current = parser.peek
    (current, dataType) match {
      case (_: StartElement, dt: DataType) => convertComplicatedType(dt)
      case (_: EndElement, _: DataType) => null
      case (c: Characters, dt: DataType) if !c.isIgnorableWhiteSpace && c.isWhiteSpace =>
        // When `Characters` is found, we need to look further to decide
        // if this is really data or space between other elements.
        val next = {
          parser.nextEvent
          parser.peek
        }
        val data = c.asCharacters().getData
        (next, dataType) match {
          case (_: EndElement, _) => if (conf.treatEmptyValuesAsNulls) null else data
          case (_: StartElement, dt: DataType) => convertComplicatedType(dt)
        }
      case (c: Characters, ArrayType(st, _)) if !c.isIgnorableWhiteSpace && !c.isWhiteSpace =>
        convertStringTo(c.asCharacters().getData, st)
      case (c: Characters, dt: DataType) if !c.isIgnorableWhiteSpace && !c.isWhiteSpace =>
        convertStringTo(c.asCharacters().getData, dt)
      case (e: XMLEvent, dt: DataType) =>
        sys.error(s"Failed to parse a value for data type $dt with event ${e.toString}")
    }
  }

  private def convertStringTo(value: String, dataType: DataType): Any = {
    dataType match {
      case LongType => signSafeToLong(value)
      case DoubleType => signSafeToDouble(value)
      case BooleanType => castTo(value, BooleanType)
      case StringType => castTo(value, StringType)
      case DateType => castTo(value, DateType)
      case TimestampType => castTo(value, TimestampType)
      case FloatType => signSafeToFloat(value)
      case ByteType => castTo(value, ByteType)
      case ShortType => castTo(value, ShortType)
      case IntegerType => signSafeToInt(value)
      case _: DecimalType => castTo(value, new DecimalType(None))
      case NullType => null
      case dataType =>
        sys.error(s"Failed to parse a value for data type $dataType.")
    }
  }

  /**
   * Parse an object as map.
   */
  private def convertMap(parser: XMLEventReader,
                         valueType: DataType,
                         conf: StaxConfiguration): Map[String, Any] = {
    val keys = ArrayBuffer.empty[String]
    val values = ArrayBuffer.empty[Any]
    var shouldStop = false
    while (!shouldStop) {
      parser.nextEvent match {
        case e: StartElement =>
          keys += e.getName.getLocalPart
          values += convertField(parser, valueType, conf)
        case _: EndElement =>
          shouldStop = checkEndElement(parser, conf)
        case _ =>
          shouldStop = shouldStop && parser.hasNext
      }
    }
    keys.zip(values).toMap
  }

  /**
   * Convert string values to required data type.
   */
  private def convertValues(valuesMap: Map[String, String],
                            schema: StructType): Map[String, Any] = {
    val convertedValuesMap = collection.mutable.Map.empty[String, Any]
    valuesMap.foreach {
      case (f, v) =>
        val nameToIndex = schema.map(_.name).zipWithIndex.toMap
        nameToIndex.get(f).foreach {
          case i =>
            convertedValuesMap(f) = convertStringTo(v, schema(i).dataType)
        }
    }
    Map(convertedValuesMap.toSeq: _*)
  }

  /**
   * Parse an object from the token stream into a new Row representing the schema.
   * Fields in the xml that are not defined in the requested schema will be dropped.
   */
  private def convertObject(parser: XMLEventReader,
                            schema: StructType,
                            conf: StaxConfiguration,
                            rootAttributes: Array[Attribute] = Array()): Row = {
    def toValuesMap(attributes: Array[Attribute]): Map[String, String] = {
      if (conf.excludeAttributeFlag) {
        Map.empty[String, String]
      } else {
        val attrFields = attributes.map(conf.attributePrefix + _.getName.getLocalPart)
        val attrValues = attributes.map(_.getValue)
        attrFields.zip(attrValues).toMap
      }
    }

    val row = new Array[Any](schema.length)
    var shouldStop = false
    while (!shouldStop) {
        parser.nextEvent match {
        case e: StartElement =>
          val nameToIndex = schema.map(_.name).zipWithIndex.toMap
          // If there are attributes, then we process them first.
          convertValues(toValuesMap(rootAttributes), schema).toSeq.foreach {
            case (f, v) =>
              nameToIndex.get(f).foreach(row.update(_, v))
          }
          val attributes = e.getAttributes.map(_.asInstanceOf[Attribute]).toArray
          // Set elements and other attributes to the row
          val field = e.asStartElement.getName.getLocalPart
          nameToIndex.get(field).foreach {
            case index =>
              val dataType = schema(index).dataType
              dataType match {
                case st: StructType if st.exists(_.name == conf.valueTag) =>
                  // If this is the element having no children, then it wraps attributes with a row
                  // So, we first need to find the field name that has the real value and then push
                  // the value.
                  row(index) = {
                    val valuesMap = convertValues(toValuesMap(attributes), st)
                    val value = {
                      val dataType = st.filter(_.name == conf.valueTag).head.dataType
                      convertField(parser, dataType, conf)
                    }
                    // The attributes are sorted therefore `TreeMap` is used.
                    val row = (TreeMap(conf.valueTag -> value) ++ valuesMap).values.toSeq
                    Row.fromSeq(row)
                  }
                case _: StructType =>
                  // If there are attributes, then we process them first.
                  convertValues(toValuesMap(attributes), schema).toSeq.foreach {
                    case (f, v) =>
                      nameToIndex.get(f).foreach(row.update(_, v))
                  }
                  row(index) = convertField(parser, dataType, conf)
                case _: ArrayType =>
                  val elements = {
                    val values = Option(row(index))
                      .map(_.asInstanceOf[ArrayBuffer[Any]])
                      .getOrElse(ArrayBuffer.empty[Any])
                    val newValue = convertField(parser, dataType, conf)
                    values :+ newValue
                  }
                  row(index) = elements
                case _ =>
                  row(index) = convertField(parser, dataType, conf)
              }
          }
        case _: EndElement =>
          shouldStop = checkEndElement(parser, conf)
        case _ =>
          shouldStop = shouldStop && parser.hasNext
      }
    }
    Row.fromSeq(row)
  }
}
