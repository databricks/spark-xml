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
import com.databricks.spark.xml.XmlOptions

/**
 * Wraps parser to iteration process.
 */
private[xml] object StaxXmlParser {
  private val logger = LoggerFactory.getLogger(StaxXmlParser.getClass)

  def parse(xml: RDD[String],
            schema: StructType,
            options: XmlOptions): RDD[Row] = {
    val failFast = options.failFastFlag
    xml.mapPartitions { iter =>
      iter.flatMap { xml =>
        // It does not have to skip for white space, since `XmlInputFormat`
        // always finds the root tag without a heading space.
        val factory = XMLInputFactory.newInstance()
        val reader = new ByteArrayInputStream(xml.getBytes)
        val parser = factory.createXMLEventReader(reader)
        try {
          val rootAttributes = {
            val rootEvent = StaxXmlParserUtils.skipUntil(parser, XMLStreamConstants.START_ELEMENT)
            rootEvent.asStartElement.getAttributes
              .map(_.asInstanceOf[Attribute]).toArray
          }
          Some(convertObject(parser, schema, options, rootAttributes))
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
   * Check if current event points the EndElement.
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
          case _: StartElement => false
          case _: Characters => checkEndElement(parser, options)
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
                                options: XmlOptions): Any = {
    def convertComplicatedType: DataType => Any = {
      case dt: StructType => convertObject(parser, dt, options)
      case MapType(StringType, vt, _) => convertMap(parser, vt, options)
      case ArrayType(st, _) => convertField(parser, st, options)
      case udt: UserDefinedType[_] => convertField(parser, udt.sqlType, options)
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
          case (_: EndElement, _) => if (options.treatEmptyValuesAsNulls) null else data
          case (_: StartElement, dt: DataType) => convertComplicatedType(dt)
          case (_: Characters, dt: DataType) =>
            convertStringTo(StaxXmlParserUtils.readDataFully(parser), dt)
        }
      case (c: Characters, ArrayType(st, _)) if !c.isIgnorableWhiteSpace && !c.isWhiteSpace =>
        convertStringTo(StaxXmlParserUtils.readDataFully(parser), st)
      case (c: Characters, dt: DataType) if !c.isIgnorableWhiteSpace && !c.isWhiteSpace =>
        convertStringTo(StaxXmlParserUtils.readDataFully(parser), dt)
      case (e: XMLEvent, dt: DataType) =>
        sys.error(s"Failed to parse a value for data type $dt with event ${e.toString}")
    }
  }

  private def convertStringTo: (String, DataType) => Any = {
    case (null, _) | (_, NullType) => null
    case (v, LongType) => signSafeToLong(v)
    case (v, DoubleType) => signSafeToDouble(v)
    case (v, BooleanType) => castTo(v, BooleanType)
    case (v, StringType) => castTo(v, StringType)
    case (v, DateType) => castTo(v, DateType)
    case (v, TimestampType) => castTo(v, TimestampType)
    case (v, FloatType) => signSafeToFloat(v)
    case (v, ByteType) => castTo(v, ByteType)
    case (v, ShortType) => castTo(v, ShortType)
    case (v, IntegerType) => signSafeToInt(v)
    case (v, _: DecimalType) => castTo(v, new DecimalType(None))
    case (_, dataType) =>
      sys.error(s"Failed to parse a value for data type $dataType.")
  }

  /**
   * Parse an object as map.
   */
  private def convertMap(parser: XMLEventReader,
                         valueType: DataType,
                         options: XmlOptions): Map[String, Any] = {
    val keys = ArrayBuffer.empty[String]
    val values = ArrayBuffer.empty[Any]
    var shouldStop = false
    while (!shouldStop) {
      parser.nextEvent match {
        case e: StartElement =>
          keys += e.getName.getLocalPart
          values += convertField(parser, valueType, options)
        case _: EndElement =>
          shouldStop = checkEndElement(parser, options)
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
                            options: XmlOptions,
                            rootAttributes: Array[Attribute] = Array()): Row = {
    def toValuesMap(attributes: Array[Attribute]): Map[String, String] = {
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
          // TODO: Simplify the complex logic below.
          nameToIndex.get(field).foreach {
            case index =>
              val dataType = schema(index).dataType
              row(index) = dataType match {
                case st: StructType if st.exists(_.name == options.valueTag) =>
                  // If this is the element having no children, then it wraps attributes with a row
                  // So, we first need to find the field name that has the real value and then push
                  // the value.
                  val valuesMap = convertValues(toValuesMap(attributes), st)
                  val value = {
                    val dataType = st.filter(_.name == options.valueTag).head.dataType
                    convertField(parser, dataType, options)
                  }
                  // The attributes are sorted therefore `TreeMap` is used.
                  val row = (TreeMap(options.valueTag -> value) ++ valuesMap).values.toSeq
                  Row.fromSeq(row)
                case _: StructType =>
                  // If there are attributes, then we process them first.
                  convertValues(toValuesMap(attributes), schema).toSeq.foreach {
                    case (f, v) =>
                      nameToIndex.get(f).foreach(row.update(_, v))
                  }
                  convertField(parser, dataType, options)
                case ArrayType(dt: DataType, _) =>
                  val elements = {
                    val values = Option(row(index))
                      .map(_.asInstanceOf[ArrayBuffer[Any]])
                      .getOrElse(ArrayBuffer.empty[Any])
                    val newValue = {
                      dt match {
                        case st: StructType  if attributes.nonEmpty =>
                          // If the given type is array but the element type is StructType,
                          // we should push and write current attributes as fields in elements
                          // in this array.
                          convertObject(parser, st, options, attributes)
                        case _ =>
                          convertField(parser, dataType, options)
                      }
                    }
                    values :+ newValue
                  }
                  elements
                case _ =>
                  convertField(parser, dataType, options)
              }
          }
        case _: EndElement =>
          shouldStop = checkEndElement(parser, options)
        case _ =>
          shouldStop = shouldStop && parser.hasNext
      }
    }
    Row.fromSeq(row)
  }
}

