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

import java.io.StringReader

import javax.xml.stream.events.{Attribute, XMLEvent}
import javax.xml.stream.events._
import javax.xml.stream._

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import scala.util.Try

import org.slf4j.LoggerFactory

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import com.databricks.spark.xml.util.TypeCast._
import com.databricks.spark.xml.XmlOptions
import com.databricks.spark.xml.util._

/**
 * Wraps parser to iteration process.
 */
private[xml] object StaxXmlParser extends Serializable {
  private val logger = LoggerFactory.getLogger(StaxXmlParser.getClass)

  def parse(
      xml: RDD[String],
      schema: StructType,
      options: XmlOptions): RDD[Row] = {

    // The logic below is borrowed from Apache Spark's FailureSafeParser.
    val corruptFieldIndex = Try(schema.fieldIndex(options.columnNameOfCorruptRecord)).toOption
    val actualSchema = StructType(schema.filterNot(_.name == options.columnNameOfCorruptRecord))
    val resultRow = new Array[Any](schema.length)
    val toResultRow: (Option[Row], String) => Row = (row, badRecord) => {
      var i = 0
      while (i < actualSchema.length) {
        val from = actualSchema(i)
        resultRow(schema.fieldIndex(from.name)) = row.map(_.get(i)).orNull
        i += 1
      }
      corruptFieldIndex.foreach(index => resultRow(index) = badRecord)
      Row.fromSeq(resultRow)
    }

    def failedRecord(
        record: String, cause: Throwable = null, partialResult: Option[Row] = None): Option[Row] = {
      // create a row even if no corrupt record column is present
      options.parseMode match {
        case FailFastMode =>
          throw new IllegalArgumentException(
            s"Malformed line in FAILFAST mode: ${record.replaceAll("\n", "")}", cause)
        case DropMalformedMode =>
          val reason = if (cause != null) s"Reason: ${cause.getMessage}" else ""
          logger.warn(s"Dropping malformed line: ${record.replaceAll("\n", "")}. $reason")
          logger.debug("Malformed line cause:", cause)
          None
        case PermissiveMode =>
          logger.debug("Malformed line cause:", cause)
          Some(toResultRow(partialResult, record))
      }
    }

    xml.mapPartitions { iter =>
      val factory = XMLInputFactory.newInstance()
      factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false)
      factory.setProperty(XMLInputFactory.IS_COALESCING, true)
      val filter = new EventFilter {
        override def accept(event: XMLEvent): Boolean =
          // Ignore comments and processing instructions
          event.getEventType match {
            case XMLStreamConstants.COMMENT | XMLStreamConstants.PROCESSING_INSTRUCTION => false
            case _ => true
          }
      }

      iter.flatMap { xml =>
        // It does not have to skip for white space, since `XmlInputFormat`
        // always finds the root tag without a heading space.
        val eventReader = factory.createXMLEventReader(new StringReader(xml))
        val parser = factory.createFilteredReader(eventReader, filter)
        try {
          val rootEvent =
            StaxXmlParserUtils.skipUntil(parser, XMLStreamConstants.START_ELEMENT)
          val rootAttributes =
            rootEvent.asStartElement.getAttributes.asScala.map(_.asInstanceOf[Attribute]).toArray
          Some(convertObject(parser, schema, options, rootAttributes))
            .orElse(failedRecord(xml))
        } catch {
          case e: PartialResultException =>
            failedRecord(xml, e.cause, Some(e.partialResult))
          case NonFatal(e) =>
            failedRecord(xml, e)
        }
      }
    }
  }

  /**
   * Parse the current token (and related children) according to a desired schema
   */
  private[xml] def convertField(
      parser: XMLEventReader,
      dataType: DataType,
      options: XmlOptions): Any = {
    def convertComplicatedType(dt: DataType): Any = dt match {
      case st: StructType => convertObject(parser, st, options)
      case MapType(StringType, vt, _) => convertMap(parser, vt, options)
      case ArrayType(st, _) => convertField(parser, st, options)
      case _: StringType => StaxXmlParserUtils.currentStructureAsString(parser)
    }

    (parser.peek, dataType) match {
      case (_: StartElement, dt: DataType) => convertComplicatedType(dt)
      case (_: EndElement, _: StringType) =>
        // Empty. It's null if these are explicitly treated as null, or "" is the null value
        if (options.treatEmptyValuesAsNulls || options.nullValue == ""){
          null
        } else {
          ""
        }
      case (_: EndElement, _: DataType) => null
      case (c: Characters, _: DataType) if c.isWhiteSpace =>
        // When `Characters` is found, we need to look further to decide
        // if this is really data or space between other elements.
        val data = c.getData
        parser.next
        parser.peek match {
          case _: StartElement => convertComplicatedType(dataType)
          case _: EndElement if data.isEmpty => null
          case _: EndElement if options.treatEmptyValuesAsNulls => null
          case _: EndElement => data
          case _ => convertField(parser, dataType, options)
        }

      case (c: Characters, ArrayType(st, _)) =>
        // For `ArrayType`, it needs to return the type of element. The values are merged later.
        convertTo(c.getData, st, options)
      case (c: Characters, st: StructType) =>
        // If a value tag is present, this can be an attribute-only element whose values is in that
        // value tag field. Or, it can be a mixed-type element with both some character elements
        // and other complex structure. Character elements are ignored.
        val attributesOnly = st.fields.forall { f =>
          f.name == options.valueTag || f.name.startsWith(options.attributePrefix)
        }
        if (attributesOnly) {
          // If everything else is an attribute column, there's no complex structure.
          // Just return the value of the character element
          val dt = st.find(_.name == options.valueTag).get.dataType
          convertTo(c.getData, dt, options)
        } else {
          // Otherwise, ignore this character element, and continue parsing the following complex
          // structure
          parser.next
          convertObject(parser, st, options)
        }
      case (c: Characters, dt: DataType) =>
        convertTo(c.getData, dt, options)
      case (e: XMLEvent, dt: DataType) =>
        throw new IllegalArgumentException(
          s"Failed to parse a value for data type $dt with event ${e.toString}")
    }
  }

  /**
   * Parse an object as map.
   */
  private def convertMap(
      parser: XMLEventReader,
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
          shouldStop = StaxXmlParserUtils.checkEndElement(parser)
        case _ => // do nothing
      }
    }
    keys.zip(values).toMap
  }

  /**
   * Convert XML attributes to a map with the given schema types.
   */
  private def convertAttributes(
      attributes: Array[Attribute],
      schema: StructType,
      options: XmlOptions): Map[String, Any] = {
    val convertedValuesMap = collection.mutable.Map.empty[String, Any]
    val valuesMap = StaxXmlParserUtils.convertAttributesToValuesMap(attributes, options)
    valuesMap.foreach { case (f, v) =>
      val nameToIndex = schema.map(_.name).zipWithIndex.toMap
      nameToIndex.get(f).foreach { i =>
        convertedValuesMap(f) = convertTo(v, schema(i).dataType, options)
      }
    }
    convertedValuesMap.toMap
  }

  /**
   * [[convertObject()]] calls this in order to convert the nested object to a row.
   * [[convertObject()]] contains some logic to find out which events are the start
   * and end of a nested row and this function converts the events to a row.
   */
  private def convertObjectWithAttributes(
      parser: XMLEventReader,
      schema: StructType,
      options: XmlOptions,
      attributes: Array[Attribute] = Array.empty): Row = {
    // TODO: This method might have to be removed. Some logics duplicate `convertObject()`
    val row = new Array[Any](schema.length)

    // Read attributes first.
    val attributesMap = convertAttributes(attributes, schema, options)

    // Then, we read elements here.
    val fieldsMap = convertField(parser, schema, options) match {
      case row: Row =>
        Map(schema.map(_.name).zip(row.toSeq): _*)
      case v if schema.fieldNames.contains(options.valueTag) =>
        // If this is the element having no children, then it wraps attributes
        // with a row So, we first need to find the field name that has the real
        // value and then push the value.
        val valuesMap = schema.fieldNames.map((_, null)).toMap
        valuesMap + (options.valueTag -> v)
      case _ => Map.empty
    }

    // Here we merge both to a row.
    val valuesMap = fieldsMap ++ attributesMap
    valuesMap.foreach { case (f, v) =>
      val nameToIndex = schema.map(_.name).zipWithIndex.toMap
      nameToIndex.get(f).foreach { row(_) = v }
    }

    if (valuesMap.isEmpty) {
      // Return an empty row with all nested elements by the schema set to null.
      Row.fromSeq(Seq.fill(schema.fieldNames.length)(null))
    } else {
      Row.fromSeq(row)
    }
  }

  /**
   * Parse an object from the event stream into a new Row representing the schema.
   * Fields in the xml that are not defined in the requested schema will be dropped.
   */
  private def convertObject(
      parser: XMLEventReader,
      schema: StructType,
      options: XmlOptions,
      rootAttributes: Array[Attribute] = Array.empty): Row = {
    val row = new Array[Any](schema.length)
    val nameToIndex = schema.map(_.name).zipWithIndex.toMap
    // If there are attributes, then we process them first.
    convertAttributes(rootAttributes, schema, options).toSeq.foreach { case (f, v) =>
      nameToIndex.get(f).foreach { row(_) = v }
    }
    var badRecordException: Option[Throwable] = None

    var shouldStop = false
    while (!shouldStop) {
      parser.nextEvent match {
        case e: StartElement => try {
          val attributes = e.getAttributes.asScala.map(_.asInstanceOf[Attribute]).toArray
          val field = e.asStartElement.getName.getLocalPart

          nameToIndex.get(field) match {
            case Some(index) => schema(index).dataType match {
              case st: StructType =>
                row(index) = convertObjectWithAttributes(parser, st, options, attributes)

              case ArrayType(dt: DataType, _) =>
                val values = Option(row(index))
                  .map(_.asInstanceOf[ArrayBuffer[Any]])
                  .getOrElse(ArrayBuffer.empty[Any])
                val newValue = dt match {
                  case st: StructType =>
                    convertObjectWithAttributes(parser, st, options, attributes)
                  case dt: DataType =>
                    convertField(parser, dt, options)
                }
                row(index) = values :+ newValue

              case dt: DataType =>
                row(index) = convertField(parser, dt, options)
            }

            case None =>
              StaxXmlParserUtils.skipChildren(parser)
          }
        } catch {
          case NonFatal(exception) if options.parseMode == PermissiveMode =>
            badRecordException = badRecordException.orElse(Some(exception))
        }

        case _: EndElement =>
          shouldStop = StaxXmlParserUtils.checkEndElement(parser)

        case _ => // do nothing
      }
    }

    if (badRecordException.isEmpty) {
      Row.fromSeq(row)
    } else {
      throw PartialResultException(Row.fromSeq(row), badRecordException.get)
    }
  }
}
