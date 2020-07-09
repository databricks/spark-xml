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

import com.databricks.spark.xml.util.TypeCast._
import com.databricks.spark.xml.util._
import com.databricks.spark.xml.{ XmlOptions, XmlPath }
import javax.xml.stream.XMLEventReader
import javax.xml.stream.events.{ Attribute, Characters, EndDocument, EndElement, StartElement, XMLEvent }
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.control.NonFatal

/**
 * Wraps parser to iteration process.
 */
private[xml] object StaxXmlParser extends Serializable {
  private val logger = LoggerFactory.getLogger(StaxXmlParser.getClass)

  def parse(xml: RDD[String],
            schema: StructType,
            options: XmlOptions): RDD[Row] = {
    xml.mapPartitions { iter =>
      val xsdSchema = Option(options.rowValidationXSDPath).map(ValidatorUtil.getSchema)
      iter.flatMap { xml =>
        doParseColumn(xml, schema, options, options.parseMode, xsdSchema)
      }
    }
  }

  def parseColumn(xml: String, schema: StructType, options: XmlOptions): Row = {
    // The user=specified schema from from_xml, etc will typically not include a
    // "corrupted record" column. In PERMISSIVE mode, which puts bad records in
    // such a column, this would cause an error. In this mode, if such a column
    // is not manually specified, then fall back to DROPMALFORMED, which will return
    // null column values where parsing fails.
    val parseMode =
    if (options.parseMode == PermissiveMode &&
      !schema.fields.exists(_.name == options.columnNameOfCorruptRecord)) {
      DropMalformedMode
    } else {
      options.parseMode
    }
    val xsdSchema = Option(options.rowValidationXSDPath).map(ValidatorUtil.getSchema)
    doParseColumn(xml, schema, options, parseMode, xsdSchema).orNull
  }

  private def doParseColumn(xml: String,
                            schema: StructType,
                            options: XmlOptions,
                            parseMode: ParseMode,
                            xsdSchema: Option[Schema]): Option[Row] = {
    try {
      xsdSchema.foreach { schema =>
        schema.newValidator().validate(new StreamSource(new StringReader(xml)))
      }
      val parser = new TrackingXmlEventReader(StaxXmlParserUtils.filteredReader(xml))

      val rootAttributes = StaxXmlParserUtils.gatherRootAttributes(parser)
      Some(convertObject(XmlPath(Seq(options.rowTag)), parser, schema, options, rootAttributes))
    } catch {
      case e: PartialResultException =>
        failedRecord(xml, options, parseMode, schema, e.cause, Some(e.partialResult))
      case NonFatal(e) =>
        failedRecord(xml, options, parseMode, schema, e)
    }
  }

  private def failedRecord(record: String,
                           options: XmlOptions,
                           parseMode: ParseMode,
                           schema: StructType,
                           cause: Throwable = null,
                           partialResult: Option[Row] = None): Option[Row] = {
    // create a row even if no corrupt record column is present
    parseMode match {
      case FailFastMode =>
        val abbreviatedRecord =
          if (record.length() > 1000) record.substring(0, 1000) + "..." else record
        throw new IllegalArgumentException(
          s"Malformed line in FAILFAST mode: ${abbreviatedRecord.replaceAll("\n", "")}", cause)
      case DropMalformedMode =>
        val reason = if (cause != null) s"Reason: ${cause.getMessage}" else ""
        val abbreviatedRecord =
          if (record.length() > 1000) record.substring(0, 1000) + "..." else record
        logger.warn(s"Dropping malformed line: ${abbreviatedRecord.replaceAll("\n", "")}. $reason")
        logger.debug("Malformed line cause:", cause)
        None
      case PermissiveMode =>
        logger.debug("Malformed line cause:", cause)
        // The logic below is borrowed from Apache Spark's FailureSafeParser.
        val corruptFieldIndex = Try(schema.fieldIndex(options.columnNameOfCorruptRecord)).toOption
        val actualSchema = StructType(schema.filterNot(_.name == options.columnNameOfCorruptRecord))
        val resultRow = new Array[Any](schema.length)
        var i = 0
        while (i < actualSchema.length) {
          val from = actualSchema(i)
          resultRow(schema.fieldIndex(from.name)) = partialResult.map(_.get(i)).orNull
          i += 1
        }
        corruptFieldIndex.foreach(index => resultRow(index) = record)
        Some(Row.fromSeq(resultRow))
    }
  }

  /**
   * Parse the current token (and related children) according to a desired schema
   */
  private[xml] def convertField(
                                 elementPath: XmlPath,
                                 parser: TrackingXmlEventReader,
                                 dataType: DataType,
                                 options: XmlOptions): Any = {
    def convertComplicatedType(dt: DataType): Any = dt match {
      case st: StructType => convertObject(elementPath, parser, st, options)
      case MapType(StringType, vt, _) => convertMap(elementPath, parser, vt, options)
      case ArrayType(st, _) => convertField(elementPath, parser, st, options)
      case _: StringType => StaxXmlParserUtils.currentStructureAsString(parser)
    }

    val peek = parser.peek
    (peek, dataType) match {
      case (_: StartElement, dt: DataType) => convertComplicatedType(dt)
      case (_: EndElement, _: StringType) =>
        // Empty. It's null if these are explicitly treated as null, or "" is the null value
        if (options.treatEmptyValuesAsNulls || options.nullValue == "") {
          null
        } else {
          ""
        }
      case (_: EndElement, _: DataType) => null
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
        val convertedField = if (attributesOnly) {
          // If everything else is an attribute column, there's no complex structure.
          // Just return the value of the character element
          val dt = st.find(_.name == options.valueTag).get.dataType
          parser.nextEvent()
          convertTo(c.getData, dt, options)
        } else {
          // Otherwise, ignore this character element, and continue parsing the following complex
          // structure
          convertObject(elementPath, parser, st, options)
        }
        convertedField
      case (c: Characters, _: DataType) if c.isWhiteSpace =>
        // When `Characters` is found, we need to look further to decide
        // if this is really data or space between other elements.
        val data = c.getData
        parser.nextEvent()
        parser.peek match {
          case _: StartElement => convertComplicatedType(dataType)
          case _: EndElement if data.isEmpty => null
          case _: EndElement if options.treatEmptyValuesAsNulls => null
          case _: EndElement => convertTo(data, dataType, options)
          case _ => convertField(elementPath, parser, dataType, options)
        }
      case (c: Characters, dt: DataType) =>
        val data = c.getData
        parser.nextEvent()
        convertTo(data, dt, options)
      case (e: XMLEvent, dt: DataType) =>
        throw new IllegalArgumentException(
          s"Failed to parse a value for data type $dt with event ${e.toString}")
    }
  }

  /**
   * Parse an object as map.
   */
  private def convertMap(
                          elementPath: XmlPath,
                          parser: TrackingXmlEventReader,
                          valueType: DataType,
                          options: XmlOptions): Map[String, Any] = {
    val keys = ArrayBuffer.empty[String]
    val values = ArrayBuffer.empty[Any]
    var shouldStop = false
    while (!shouldStop) {
      parser.nextEvent match {
        case e: StartElement =>
          keys += e.getName.getLocalPart
          values += convertField(elementPath, parser, valueType, options)
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
                                           elementPath: XmlPath,
                                           parser: TrackingXmlEventReader,
                                           schema: StructType,
                                           options: XmlOptions,
                                           attributes: Array[Attribute] = Array.empty): Row = {
    // TODO: This method might have to be removed. Some logics duplicate `convertObject()`
    val row = new Array[Any](schema.length)

    // Read attributes first.
    val attributesMap = convertAttributes(attributes, schema, options)

    // Then, we read elements here.
    val fieldsMap = convertField(elementPath, parser, schema, options) match {
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
      nameToIndex.get(f).foreach {
        row(_) = v
      }
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
                             elementPath: XmlPath,
                             parser: TrackingXmlEventReader,
                             schema: StructType,
                             options: XmlOptions,
                             rootAttributes: Array[Attribute] = Array.empty): Row = {
    val row = new Array[Any](schema.length)
    val nameToIndex = schema.map(_.name).map(name => elementPath.child(name)).zipWithIndex.toMap
    // If there are attributes, then we process them first.
    convertAttributes(rootAttributes, schema, options).toSeq.foreach { case (attributeName, attributeValue) =>
      nameToIndex.get(elementPath.child(attributeName)).foreach {
        row(_) = attributeValue
      }
    }
    var badRecordException: Option[Throwable] = None

    var shouldStop = false
    while (!shouldStop) {
      parser.nextEvent match {
        case e: StartElement => try {
          val attributes = e.getAttributes.asScala.map(_.asInstanceOf[Attribute]).toArray
          val field = e.asStartElement.getName.getLocalPart
          nameToIndex.get(elementPath.child(field)) match {
            case Some(index) => schema(index).dataType match {
              case st: StructType =>
                row(index) = convertObjectWithAttributes(elementPath.child(field), parser, st, options, attributes)

              case ArrayType(dt: DataType, _) =>
                val values = Option(row(index))
                  .map(_.asInstanceOf[ArrayBuffer[Any]])
                  .getOrElse(ArrayBuffer.empty[Any])
                val newValue = dt match {
                  case st: StructType =>
                    convertObjectWithAttributes(elementPath.child(field), parser, st, options, attributes)
                  case dt: DataType =>
                    convertField(elementPath.child(field), parser, dt, options)
                }
                row(index) = values :+ newValue

              case dt: DataType =>
                row(index) = convertField(elementPath.child(field), parser, dt, options)
            }

            case None =>
              parser.nextEvent()
              StaxXmlParserUtils.skipChildren(field, parser)
          }
        } catch {
          case NonFatal(exception) if options.parseMode == PermissiveMode =>
            badRecordException = badRecordException.orElse(Some(exception))
        }

        case c: Characters if !c.isWhiteSpace =>
          nameToIndex.get(elementPath.child(options.valueTag)).map(index => index -> schema(index).dataType).foreach {
            case (index, ArrayType(_: StringType, _)) =>
              val existingValues = Option(row(index))
                .map(_.asInstanceOf[ArrayBuffer[String]])
                .getOrElse(ArrayBuffer.empty[String])
              row(index) = existingValues :+ c.getData

            case (index, StringType) =>
              row(index) = Option(row(index)).getOrElse("") + c.getData
          }

        case _: EndElement =>
          shouldStop = elementPath.isChildOf(parser.currentPath)

        case _: EndDocument =>
          shouldStop = true

        case _ => // do nothing
      }
    }

    if (badRecordException.isEmpty) {
      Row.fromSeq(row)
    } else {
      throw PartialResultException(Row.fromSeq(row), badRecordException.get)
    }
  }

  class TrackingXmlEventReader(delegateReader: XMLEventReader) extends XMLEventReader with Iterator[XMLEvent] {
    var currentPath: XmlPath = XmlPath.root

    override def nextEvent(): XMLEvent = {
      val nextEvent = delegateReader.nextEvent()
      nextEvent match {
        case startElement: StartElement => currentPath = currentPath.child(startElement.getName.getLocalPart)
        case endElement: EndElement if currentPath.leafName == endElement.getName.getLocalPart => currentPath = currentPath.parent
        case endElement: EndElement => throw new IllegalStateException(s"Found EndElement $endElement but we should be in $currentPath")
        case _ => // ignore
      }

      nextEvent
    }

    override def hasNext: Boolean = delegateReader.hasNext

    override def peek(): XMLEvent = delegateReader.peek

    override def getElementText: String = delegateReader.getElementText

    override def nextTag(): XMLEvent = delegateReader.nextTag

    override def getProperty(s: String): AnyRef = delegateReader.getProperty(s)

    override def close(): Unit = delegateReader.close()

    override def next() = throw new NotImplementedError(s"next() on ${this.getClass.getSimpleName} is not implemented")
  }

}
