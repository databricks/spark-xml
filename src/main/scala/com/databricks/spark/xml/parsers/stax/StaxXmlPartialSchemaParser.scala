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
package com.databricks.spark.xml.parsers.stax

import java.io.ByteArrayInputStream
import javax.xml.stream.events._
import javax.xml.stream.{XMLStreamException, XMLStreamConstants, XMLEventReader, XMLInputFactory}

import org.slf4j.LoggerFactory

import scala.collection.Seq
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import com.databricks.spark.xml.util.InferSchema
import com.databricks.spark.xml.util.TypeCast._
import com.databricks.spark.xml.parsers.stax.StaxXmlParser._

private[xml] object StaxXmlPartialSchemaParser {
  private val logger = LoggerFactory.getLogger(StaxXmlPartialSchemaParser.getClass)

  def parse(xml: RDD[String],
            samplingRatio: Double,
            conf: StaxConfiguration): RDD[DataType] = {
    require(samplingRatio > 0, s"samplingRatio ($samplingRatio) should be greater than 0")
    val schemaData = if (samplingRatio > 0.99) {
      xml
    } else {
      xml.sample(withReplacement = false, samplingRatio, 1)
    }
    val failFast = conf.failFastFlag
    schemaData.mapPartitions { iter =>
      iter.flatMap { xml =>
        // It does not have to skip for white space, since [[XmlInputFormat]]
        // always finds the root tag without a heading space.
        val factory = XMLInputFactory.newInstance()
        val reader = new ByteArrayInputStream(xml.getBytes)
        val parser = factory.createXMLEventReader(reader)
        try {
          val rootEvent = skipUntil(parser, XMLStreamConstants.START_ELEMENT)
          val rootAttributes = rootEvent.asStartElement.getAttributes
            .map(_.asInstanceOf[Attribute]).toArray
          Some(inferObject(parser, conf, rootAttributes))
        } catch {
          case _: java.lang.NumberFormatException | _: IllegalArgumentException if !failFast =>
            logger.warn("Number format exception. " +
              s"Dropping malformed line: ${xml.replaceAll("\n", "")}")
            None
          case _: java.text.ParseException if !failFast =>
            logger.warn("Parse exception. " +
              s"Dropping malformed line: ${xml.replaceAll("\n", "")}")
            None
          case _: XMLStreamException if !failFast =>
            logger.warn(s"Dropping malformed row: ${xml.replaceAll("\n", "")}")
            None
          case _: XMLStreamException if failFast =>
            throw new RuntimeException(s"Malformed row (failing fast): ${xml.replaceAll("\n", "")}")
        }
      }
    }
  }

  /**
   * Infer Spark-type from string value.
   */
  def inferTypeFromString(value: String): DataType = {
    if (Option(value).isEmpty) {
      NullType
    } else if (isLong(value)) {
      LongType
    } else if (isInteger(value)) {
      IntegerType
    } else if (isDouble(value)) {
      DoubleType
    } else if (isBoolean(value)) {
      BooleanType
    } else if (isTimestamp(value)) {
      TimestampType
    } else {
      StringType
    }
  }

  private def inferField(parser: XMLEventReader, conf: StaxConfiguration): DataType = {
    val current = parser.peek
    current match {
      case _: EndElement =>
        NullType
      case _: StartElement =>
        inferObject(parser, conf)
      case c: Characters if !c.isIgnorableWhiteSpace && c.isWhiteSpace =>
        // When `Characters` is found, we need to look further to decide
        // if this is really data or space between other elements.
        val next = {
          parser.nextEvent
          parser.peek
        }
        next match {
          case _: EndElement =>
            if (conf.treatEmptyValuesAsNulls) {
              NullType
            } else {
              StringType
            }
          case _: StartElement =>
            inferObject(parser, conf)
        }
      case c: Characters if !c.isIgnorableWhiteSpace && !c.isWhiteSpace =>
        // This means data exists
        inferTypeFromString(c.asCharacters().getData)

      case e: XMLEvent =>
        sys.error(s"Failed to parse data with unexpected event ${e.toString}")
    }
  }

  /**
   * Infer the type of a xml document from the parser's token stream
   */
  def inferObject(parser: XMLEventReader,
                  conf: StaxConfiguration,
                  rootAttributes: Array[Attribute] = Array()): DataType = {
    def toFieldsZipValues(attributes: Array[Attribute]): Seq[(String, String)] = {
      if (conf.excludeAttributeFlag){
        Seq()
      } else {
        val attrFields = attributes.map(conf.attributePrefix + _.getName.getLocalPart)
        val attrDataTypes = attributes.map(_.getValue)
        attrFields.zip(attrDataTypes)
      }
    }

    val builder = Seq.newBuilder[StructField]
    val nameToDataTypes = collection.mutable.Map.empty[String, ArrayBuffer[DataType]]
    var shouldStop = false
    while (!shouldStop) {
      parser.nextEvent match {
        case e: StartElement =>
          // If there are attributes, then we should process them first.
          toFieldsZipValues(rootAttributes).foreach {
            case (f, v) =>
              nameToDataTypes += (f -> ArrayBuffer(inferTypeFromString(v)))
          }
          val fieldsZipValues = {
            val attributes = e.getAttributes.map(_.asInstanceOf[Attribute]).toArray
            toFieldsZipValues(attributes)
          }
          val field = e.asStartElement.getName.getLocalPart
          val inferredType = inferField(parser, conf)
          inferredType match {
            case st: StructType =>
              val nestedBuilder = Seq.newBuilder[StructField]
              nestedBuilder ++= st.fields
              fieldsZipValues.foreach {
                case (f, v) =>
                  nestedBuilder += StructField(f, inferTypeFromString(v), nullable = true)
              }
              val dataTypes = nameToDataTypes.getOrElse(field, ArrayBuffer.empty[DataType])
              dataTypes += StructType(nestedBuilder.result().sortBy(_.name))
              nameToDataTypes += (field -> dataTypes)
            case _ if fieldsZipValues.nonEmpty =>
              // We need to wrap the attributes with a wrapper.
              val nestedBuilder = Seq.newBuilder[StructField]
              fieldsZipValues.foreach {
                case (f, v) =>
                  nestedBuilder += StructField(f, inferTypeFromString(v), nullable = true)
              }
              nestedBuilder += StructField(conf.valueTag, inferredType, nullable = true)
              nameToDataTypes +=
                (field -> ArrayBuffer(StructType(nestedBuilder.result().sortBy(_.name))))
            case _ =>
              val dataTypes = nameToDataTypes.getOrElse(field, ArrayBuffer.empty[DataType])
              dataTypes += inferredType
              nameToDataTypes += (field -> dataTypes)
          }
        case _: EndElement =>
          shouldStop = checkEndElement(parser, conf)
        case _ =>
          shouldStop = shouldStop && parser.hasNext
      }
    }
    // We need to manually merges all the [[ArrayType]]s.
    nameToDataTypes.foreach{
      case (field, dataTypes) if dataTypes.length > 1 =>
        val elementType = dataTypes.reduceLeft(InferSchema.compatibleType)
        builder += StructField(field, ArrayType(elementType), nullable = true)
      case (field, dataTypes) =>
        builder += StructField(field, dataTypes.head, nullable = true)
    }
    StructType(builder.result().sortBy(_.name))
  }
}
