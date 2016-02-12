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
      val factory = XMLInputFactory.newInstance()
      factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false)
      factory.setProperty(XMLInputFactory.IS_COALESCING, true)
      iter.flatMap { xml =>
        // It does not have to skip for white space, since `XmlInputFormat`
        // always finds the root tag without a heading space.
        val reader = new ByteArrayInputStream(xml.getBytes)
        val parser = factory.createXMLEventReader(reader)
        try {
          StaxXmlParserUtils.skipUntil(parser, XMLStreamConstants.START_ELEMENT)
          val rootEvent = parser.nextEvent()
          val rootAttributes =
            rootEvent.asStartElement.getAttributes.map(_.asInstanceOf[Attribute]).toArray
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

    (parser.peek, dataType) match {
      case (_: StartElement, dt: DataType) => convertComplicatedType(dt)
      case (_: EndElement, _: DataType) => null
      case (c: Characters, dt: DataType) if c.isWhiteSpace =>
        // When `Characters` is found, we need to look further to decide
        // if this is really data or space between other elements.
        val data = c.getData
        parser.nextEvent()
        (parser.peek, dataType) match {
          case (_: StartElement, dt: DataType) => convertComplicatedType(dt)
          case (_: EndElement, _) if data.isEmpty => null
          case (_: EndElement, _) if options.treatEmptyValuesAsNulls => null
          case (_: EndElement, _: DataType) => data
        }

      case (c: Characters, ArrayType(st, _)) =>
        // For `ArrayType`, it needs to return the type of element. The values are merged later.
        convertStringTo(c.getData, st)
      case (c: Characters, st: StructType) =>
        // This case can be happen when current data type is inferred as `StructType`
        // due to `valueTag` for elements having attributes but no child.
        val dt = st.filter(_.name == options.valueTag).head.dataType
        convertStringTo(c.getData, dt)
      case (c: Characters, dt: DataType) =>
        convertStringTo(c.getData, dt)
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
          shouldStop = StaxXmlParserUtils.checkEndElement(parser, options)
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
    val row = new Array[Any](schema.length)
    var shouldStop = false
    while (!shouldStop) {
      parser.nextEvent match {
        case e: StartElement =>
          val nameToIndex = schema.map(_.name).zipWithIndex.toMap
          // If there are attributes, then we process them first.
          val rootValuesMap =
            convertValues(StaxXmlParserUtils.toValuesMap(rootAttributes, options), schema)
          rootValuesMap.toSeq.foreach {
            case (f, v) =>
              nameToIndex.get(f).foreach(row.update(_, v))
          }
          val attributes = e.getAttributes.map(_.asInstanceOf[Attribute]).toArray
          val valuesMap = StaxXmlParserUtils.toValuesMap(attributes, options)

          // Set elements and other attributes to the row
          val field = e.asStartElement.getName.getLocalPart
          nameToIndex.get(field).foreach {
            case index =>
              val dataType = schema(index).dataType
              row(index) = dataType match {
                case st: StructType =>
                  // The fields are sorted so `TreeMap` is used.
                  val fields = convertField(parser, st, options) match {
                    case row: Row =>
                      TreeMap(st.map(_.name).zip(row.toSeq): _*)
                    case v if st.exists(_.name == options.valueTag) =>
                      // If this is the element having no children, then it wraps attributes
                      // with a row So, we first need to find the field name that has the real
                      // value and then push the value.
                      TreeMap(options.valueTag -> v)
                    case null => TreeMap()
                  }
                  val convertedValuesMap = convertValues(valuesMap, st)
                  val row = (fields ++ convertedValuesMap).values.toSeq
                  Row.fromSeq(row)

                case ArrayType(dt: DataType, _) =>
                  val values = Option(row(index))
                    .map(_.asInstanceOf[ArrayBuffer[Any]])
                    .getOrElse(ArrayBuffer.empty[Any])
                  val newValue = {
                    dt match {
                      case st: StructType  if valuesMap.nonEmpty =>
                        // If the given type is array but the element type is StructType,
                        // we should push and write current attributes as fields in elements
                        // in this array.
                        convertObject(parser, st, options, attributes)
                      case _ =>
                        convertField(parser, dataType, options)
                    }
                  }
                  values :+ newValue

                case _ =>
                  convertField(parser, dataType, options)
              }
          }

        case _: EndElement =>
          shouldStop = StaxXmlParserUtils.checkEndElement(parser, options)

        case _ =>
          shouldStop = shouldStop && parser.hasNext
      }
    }
    Row.fromSeq(row)
  }
}

