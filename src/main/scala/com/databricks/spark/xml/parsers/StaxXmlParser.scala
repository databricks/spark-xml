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
import javax.xml.stream._

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.util.control.NonFatal

import org.slf4j.LoggerFactory

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import com.databricks.spark.xml.util.TypeCast._
import com.databricks.spark.xml.{RowTracker, XmlOptions}

/**
 * Wraps parser to iteration process.
 */
private[xml] object StaxXmlParser {
  private val logger = LoggerFactory.getLogger(StaxXmlParser.getClass)

  def parse[T](
                xml: RDD[T],
                schema: StructType,
                options: XmlOptions, fn: T => String = (t: String) => t): RDD[Row] = {

    def failedRecord(record: String): Option[Row] = {
      // create a row even if no corrupt record column is present
      if (options.failFast) {
        throw new RuntimeException(
          s"Malformed line in FAILFAST mode: ${record.toString.replaceAll("\n", "")}")
      } else if (options.dropMalformed) {
        logger.warn(s"Dropping malformed line: ${record.toString.replaceAll("\n", "")}")
        None
      } else {
        val row = new Array[Any](schema.length)
        val nameToIndex = schema.map(_.name).zipWithIndex.toMap
        nameToIndex.get(options.columnNameOfCorruptRecord).foreach { corruptIndex =>
          require(schema(corruptIndex).dataType == StringType)
          row.update(corruptIndex, record)
        }
        Some(Row.fromSeq(row))
      }
    }

    xml.mapPartitions { iter =>
      val factory = XMLInputFactory.newInstance()
      factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false)
      factory.setProperty(XMLInputFactory.IS_COALESCING, true)
      val filter = new EventFilter {
        override def accept(event: XMLEvent): Boolean =
        // Ignore comments. This library does not treat comments.
          event.getEventType != XMLStreamConstants.COMMENT
      }

      iter.flatMap { xmlItem =>
        val xml = fn(xmlItem)
        // It does not have to skip for white space, since `XmlInputFormat`
        // always finds the root tag without a heading space.
        val reader = new ByteArrayInputStream(xml.getBytes)
        val eventReader = factory.createXMLEventReader(reader)
        val parser = factory.createFilteredReader(eventReader, filter)
        val rowTracker: RowTracker = new RowTracker(options)
        try {
          val rootEvent =
            StaxXmlParserUtils.skipUntil(parser, XMLStreamConstants.START_ELEMENT)
          val rootAttributes =
            rootEvent.asStartElement.getAttributes.map(_.asInstanceOf[Attribute]).toArray
          xmlItem match {
            case _: String =>
              Some(convertObject(parser, schema, options, rowTracker, rootAttributes)())
                .orElse(failedRecord(xml))
            case inputRow: Row =>
              Some(convertObject(
                parser, schema, options, rowTracker, rootAttributes)(Some(inputRow)))
                .orElse(failedRecord(xml))
          }
        } catch {
          case NonFatal(_) =>
            failedRecord(xml)
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
      options: XmlOptions,
      rowTracker: RowTracker) : Any = {
    def convertComplicatedType: DataType => Any = {
      case dt: StructType => convertObject(parser, dt, options, rowTracker)()
      case MapType(StringType, vt, _) => convertMap(parser, vt, options, rowTracker)
      case ArrayType(st, _) => convertField(parser, st, options, rowTracker)
      case _: StringType => StaxXmlParserUtils.currentStructureAsString(parser)
    }

    (parser.peek, dataType) match {
      case (_: StartElement, dt: DataType) => convertComplicatedType(dt)
      case (_: EndElement, _: DataType) => null
      case (c: Characters, dt: DataType) if c.isWhiteSpace =>
        // When `Characters` is found, we need to look further to decide
        // if this is really data or space between other elements.
        val data = c.getData
        parser.next
        parser.peek match {
          case _: StartElement => convertComplicatedType(dataType)
          case _: EndElement if data.isEmpty => null
          case _: EndElement if options.treatEmptyValuesAsNulls => null
          case _: EndElement => data
          case _ => convertField(parser, dataType, options, rowTracker)
        }

      case (c: Characters, ArrayType(st, _)) =>
        // For `ArrayType`, it needs to return the type of element. The values are merged later.
        convertTo(c.getData, st, options, rowTracker)
      case (c: Characters, st: StructType) =>
        // This case can be happen when current data type is inferred as `StructType`
        // due to `valueTag` for elements having attributes but no child.
        val dt = st.filter(_.name == options.valueTag).head.dataType
        convertTo(c.getData, dt, options, rowTracker)
      case (c: Characters, dt: DataType) =>
        convertTo(c.getData, dt, options, rowTracker)
      case (e: XMLEvent, dt: DataType) =>
        sys.error(s"Failed to parse a value for data type $dt with event ${e.toString}")
    }
  }

  /**
   * Parse an object as map.
   */
  private def convertMap(
      parser: XMLEventReader,
      valueType: DataType,
      options: XmlOptions,
      rowTracker: RowTracker): Map[String, Any] = {
    val keys = ArrayBuffer.empty[String]
    val values = ArrayBuffer.empty[Any]
    var shouldStop = false
    while (!shouldStop) {
      parser.nextEvent match {
        case e: StartElement =>
          keys += e.getName.getLocalPart
          values += convertField(parser, valueType, options, rowTracker)
        case _: EndElement =>
          shouldStop = StaxXmlParserUtils.checkEndElement(parser)
        case _ =>
          shouldStop = shouldStop && parser.hasNext
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
      options: XmlOptions,
      rowTracker: RowTracker): Map[String, Any] = {
    val convertedValuesMap = collection.mutable.Map.empty[String, Any]
    val valuesMap = StaxXmlParserUtils.convertAttributesToValuesMap(attributes, options)
    val nameToIndex = schema.map(_.name).zipWithIndex.toMap
    valuesMap.foreach { case (f, v) =>
      rowTracker.pushPath(f)
      if (nameToIndex.contains(f)) {
        nameToIndex.get(f).foreach { i =>
          convertedValuesMap(f) = convertTo(v, schema(i).dataType, options, rowTracker)
        }
      } else {
        rowTracker.pushExtra()
      }
        rowTracker.popPath()
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
      rowTracker: RowTracker,
      attributes: Array[Attribute] = Array.empty): Row = {
    // TODO: This method might have to be removed. Some logics duplicate `convertObject()`
    val row = new Array[Any](schema.length)

    // Read attributes first.
    val attributesMap = convertAttributes(attributes, schema, options, rowTracker)

    // Then, we read elements here.
    val fieldsMap = convertField(parser, schema, options, rowTracker) match {
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
    val nameToIndex = schema.map(_.name).zipWithIndex.toMap

    valuesMap.foreach { case (f, v) =>
      nameToIndex.get(f).foreach { row(_) = v }
    }

    // Return null rather than empty row. For nested structs empty row causes
    // ArrayOutOfBounds exceptions when executing an action.
    if (valuesMap.isEmpty) {
      null
    } else {
      Row.fromSeq(row)
    }
  }

  /**
   * Parse an object from the event stream into a new Row representing the schema.
   * Fields in the xml that are not defined in the requested schema will be dropped unless
    * the columnNameOfExtraFields option is set; then, the xpaths of undefined fields will be
    * stored as an array within that column.
   */
  private def convertObject(
      parser: XMLEventReader,
      schema: StructType,
      options: XmlOptions,
      rowTracker: RowTracker,
      rootAttributes: Array[Attribute] = Array.empty)(inputRow: Option[Row] = None ) : Row = {

    val row = new Array[Any](schema.length)
    var shouldStop = false
    val nameToIndex = schema.map(_.name).zipWithIndex.toMap

    while (!shouldStop) {
      parser.nextEvent match {
        case e: StartElement =>

          // If there are attributes, then we process them first.
          convertAttributes(rootAttributes, schema, options, rowTracker).toSeq.foreach {
            case (f, v) => nameToIndex.get(f).foreach { row(_) = v }
          }

          val attributes = e.getAttributes.map(_.asInstanceOf[Attribute]).toArray
          val field = e.asStartElement.getName.getLocalPart

          nameToIndex.get(field) match {
            case Some(index) =>
              rowTracker.pushPath(field)
              schema(index).dataType match {
                case st: StructType =>
                  row(index) = convertObjectWithAttributes(
                    parser, st, options, rowTracker, attributes)

                case ArrayType(dt: DataType, _) =>
                  val values = Option(row(index))
                    .map(_.asInstanceOf[ArrayBuffer[Any]])
                    .getOrElse(ArrayBuffer.empty[Any])
                  val newValue = {
                    dt match {
                      case st: StructType =>
                        convertObjectWithAttributes(parser, st, options, rowTracker, attributes)
                      case dt: DataType =>
                        // TODO We currently don't track extra attributes if the field they're
                        //    attached to was previously classified as a non-StructType
                        convertField(parser, dt, options, rowTracker)
                    }
                  }
                  row(index) = values :+ newValue

                case dt: DataType =>
                  row(index) = convertField(parser, dt, options, rowTracker)
              }
              rowTracker.popPath()

            case None =>
              rowTracker.pushExtra(field)
              StaxXmlParserUtils.skipChildren(parser)
          }

        case _: EndElement =>
          shouldStop = StaxXmlParserUtils.checkEndElement(parser)

        case _ =>
          shouldStop = shouldStop && parser.hasNext
      }
    }
    if (options.columnNameOfExtraFields != null &&
      nameToIndex.contains(options.columnNameOfExtraFields)) {
      row(nameToIndex(options.columnNameOfExtraFields)) = rowTracker.extraFields
    }
    if (options.columnNameOfCorruptFields != null &&
      nameToIndex.contains(options.columnNameOfCorruptFields)) {
      row(nameToIndex(options.columnNameOfCorruptFields)) = rowTracker.errorFields
    }

    // additional handling for adding input row's column to output row
    inputRow.map { in =>
      val nameToIndex = schema.map(_.name).zipWithIndex.toMap
      val refSchema = in.schema.fieldNames.toSeq.filterNot(_ == options.contentCol);
      refSchema.foreach { elem =>
        nameToIndex.get(elem).foreach { x => row.update(x, in.getAs(elem)) }
      }
    }

    Row.fromSeq(row)
  }
}
