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
package com.databricks.spark.xml.util

import java.io.StringReader
import java.util.Comparator

import javax.xml.stream._
import javax.xml.stream.events._

import scala.collection.JavaConverters._
import scala.collection.Seq
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import com.databricks.spark.xml.XmlOptions
import com.databricks.spark.xml.parsers.StaxXmlParserUtils
import com.databricks.spark.xml.util.TypeCast._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

private[xml] object InferSchema {

  /**
   * Copied from internal Spark api
   * [[org.apache.spark.sql.catalyst.analysis.TypeCoercion]]
   */
  private val numericPrecedence: IndexedSeq[DataType] =
    IndexedSeq[DataType](
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType,
      TimestampType,
      DecimalType.SYSTEM_DEFAULT)

  private val findTightestCommonTypeOfTwo: (DataType, DataType) => Option[DataType] = {
    case (t1, t2) if t1 == t2 => Some(t1)

    // Promote numeric types to the highest of the two
    case (t1, t2) if Seq(t1, t2).forall(numericPrecedence.contains) =>
      val index = numericPrecedence.lastIndexWhere(t => t == t1 || t == t2)
      Some(numericPrecedence(index))

    case _ => None
  }

  private[this] val structFieldComparator = new Comparator[StructField] {
    override def compare(o1: StructField, o2: StructField): Int = {
      o1.name.compare(o2.name)
    }
  }

  /**
   * Infer the type of a collection of XML records in three stages:
   *   1. Infer the type of each record
   *   2. Merge types by choosing the lowest type necessary to cover equal keys
   *   3. Replace any remaining null fields with string, the top type
   */
  def infer(xml: RDD[String], options: XmlOptions): StructType = {
    val schemaData = if (options.samplingRatio > 0.99) {
      xml
    } else {
      xml.sample(withReplacement = false, options.samplingRatio, 1)
    }
    // perform schema inference on each row and merge afterwards
    val rootType = schemaData.mapPartitions { iter =>
      val factory = XMLInputFactory.newInstance()
      factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false)
      factory.setProperty(XMLInputFactory.IS_COALESCING, true)
      val filter = new EventFilter {
        override def accept(event: XMLEvent): Boolean =
          // Ignore comments. This library does not treat comments.
          event.getEventType != XMLStreamConstants.COMMENT
      }

      iter.flatMap { xml =>
        // It does not have to skip for white space, since [[XmlInputFormat]]
        // always finds the root tag without a heading space.
        val eventReader = factory.createXMLEventReader(new StringReader(xml))
        val parser = factory.createFilteredReader(eventReader, filter)
        try {
          val rootEvent =
            StaxXmlParserUtils.skipUntil(parser, XMLStreamConstants.START_ELEMENT)
          val rootAttributes =
            rootEvent.asStartElement.getAttributes.asScala.map(_.asInstanceOf[Attribute]).toArray

          Some(inferObject(parser, options, rootAttributes))
        } catch {
          case NonFatal(_) if options.parseMode == PermissiveMode =>
            Some(StructType(Seq(StructField(options.columnNameOfCorruptRecord, StringType))))
          case NonFatal(_) =>
            None
        }
      }
    }.fold(StructType(Seq()))(compatibleType(options))

    canonicalizeType(rootType) match {
      case Some(st: StructType) => st
      case _ =>
        // canonicalizeType erases all empty structs, including the only one we want to keep
        StructType(Seq())
    }
  }

  private def inferFrom(datum: String, options: XmlOptions): DataType = {
    val value = if (datum != null && options.ignoreSurroundingSpaces) {
      datum.trim()
    } else {
      datum
    }

    value match {
      case null => NullType
      case v if v.isEmpty => NullType
      case v if isLong(v) => LongType
      case v if isInteger(v) => IntegerType
      case v if isDouble(v) => DoubleType
      case v if isBoolean(v) => BooleanType
      case v if isTimestamp(v) => TimestampType
      case _ => StringType
    }
  }

  private def inferField(parser: XMLEventReader, options: XmlOptions): DataType = {
    parser.peek match {
      case _: EndElement => NullType
      case _: StartElement => inferObject(parser, options)
      case c: Characters if c.isWhiteSpace =>
        // When `Characters` is found, we need to look further to decide
        // if this is really data or space between other elements.
        val data = c.getData
        parser.nextEvent()
        parser.peek match {
          case _: StartElement => inferObject(parser, options)
          case _: EndElement if data.isEmpty => NullType
          case _: EndElement if options.treatEmptyValuesAsNulls => NullType
          case _: EndElement => StringType
          case _ => inferField(parser, options)
        }
      case c: Characters if !c.isWhiteSpace =>
        // This means data exists
        inferFrom(c.getData, options)
      case e: XMLEvent =>
        sys.error(s"Failed to parse data with unexpected event $e")
    }
  }

  /**
   * Infer the type of a xml document from the parser's token stream
   */
  private def inferObject(
      parser: XMLEventReader,
      options: XmlOptions,
      rootAttributes: Array[Attribute] = Array.empty): DataType = {
    val builder = Array.newBuilder[StructField]
    val nameToDataType = collection.mutable.Map.empty[String, ArrayBuffer[DataType]]
    // If there are attributes, then we should process them first.
    val rootValuesMap =
      StaxXmlParserUtils.convertAttributesToValuesMap(rootAttributes, options)
    rootValuesMap.foreach {
      case (f, v) =>
        nameToDataType += (f -> ArrayBuffer(inferFrom(v, options)))
    }
    var shouldStop = false
    while (!shouldStop) {
      parser.nextEvent match {
        case e: StartElement =>
          val attributes = e.getAttributes.asScala.map(_.asInstanceOf[Attribute]).toArray
          val valuesMap = StaxXmlParserUtils.convertAttributesToValuesMap(attributes, options)
          val inferredType = inferField(parser, options) match {
            case st: StructType if valuesMap.nonEmpty =>
              // Merge attributes to the field
              val nestedBuilder = Array.newBuilder[StructField]
              nestedBuilder ++= st.fields
              valuesMap.foreach {
                case (f, v) =>
                  nestedBuilder += StructField(f, inferFrom(v, options), nullable = true)
              }
              val nesterBuilderArr = nestedBuilder.result()
              java.util.Arrays.sort(nesterBuilderArr, structFieldComparator)
              StructType(nesterBuilderArr)

            case dt: DataType if valuesMap.nonEmpty =>
              // We need to manually add the field for value.
              val nestedBuilder = Array.newBuilder[StructField]
              nestedBuilder += StructField(options.valueTag, dt, nullable = true)
              valuesMap.foreach {
                case (f, v) =>
                  nestedBuilder += StructField(f, inferFrom(v, options), nullable = true)
              }
              val nesterBuilderArr = nestedBuilder.result()
              java.util.Arrays.sort(nesterBuilderArr, structFieldComparator)
              StructType(nesterBuilderArr)

            case dt: DataType => dt
          }
          // Add the field and datatypes so that we can check if this is ArrayType.
          val field = e.asStartElement.getName.getLocalPart
          val dataTypes = nameToDataType.getOrElse(field, ArrayBuffer.empty[DataType])
          dataTypes += inferredType
          nameToDataType += (field -> dataTypes)

        case _: EndElement =>
          shouldStop = StaxXmlParserUtils.checkEndElement(parser)

        case _ =>
          shouldStop = shouldStop && parser.hasNext
      }
    }
    // We need to manually merges the fields having the sames so that
    // This can be inferred as ArrayType.
    nameToDataType.foreach {
      case (field, dataTypes) if dataTypes.length > 1 =>
        val elementType = dataTypes.reduceLeft(InferSchema.compatibleType(options))
        builder += StructField(field, ArrayType(elementType), nullable = true)
      case (field, dataTypes) =>
        builder += StructField(field, dataTypes.head, nullable = true)
    }

    val fields = builder.result()
    // Note: other code relies on this sorting for correctness, so don't remove it!
    java.util.Arrays.sort(fields, structFieldComparator)
    StructType(fields)
  }

  /**
   * Convert NullType to StringType and remove StructTypes with no fields
   */
  private def canonicalizeType(dt: DataType): Option[DataType] = dt match {
    case at @ ArrayType(elementType, _) =>
      for {
        canonicalType <- canonicalizeType(elementType)
      } yield {
        at.copy(canonicalType)
      }

    case StructType(fields) =>
      val canonicalFields = for {
        field <- fields if field.name.nonEmpty
        canonicalType <- canonicalizeType(field.dataType)
      } yield {
        field.copy(dataType = canonicalType)
      }

      if (canonicalFields.nonEmpty) {
        Some(StructType(canonicalFields))
      } else {
        // per SPARK-8093: empty structs should be deleted
        None
      }

    case NullType => Some(StringType)
    case other => Some(other)
  }

  /**
   * Returns the most general data type for two given data types.
   */
  private[xml] def compatibleType(options: XmlOptions)(t1: DataType, t2: DataType): DataType = {
    // TODO: Optimise this logic.
    findTightestCommonTypeOfTwo(t1, t2).getOrElse {
      // t1 or t2 is a StructType, ArrayType, or an unexpected type.
      (t1, t2) match {
        // Double support larger range than fixed decimal, DecimalType.Maximum should be enough
        // in most case, also have better precision.
        case (DoubleType, _: DecimalType) =>
          DoubleType
        case (_: DecimalType, DoubleType) =>
          DoubleType
        case (t1: DecimalType, t2: DecimalType) =>
          val scale = math.max(t1.scale, t2.scale)
          val range = math.max(t1.precision - t1.scale, t2.precision - t2.scale)
          if (range + scale > 38) {
            // DecimalType can't support precision > 38
            DoubleType
          } else {
            DecimalType(range + scale, scale)
          }

        case (StructType(fields1), StructType(fields2)) =>
          val newFields = (fields1 ++ fields2).groupBy(_.name).map {
            case (name, fieldTypes) =>
              val dataType = fieldTypes.view.map(_.dataType).reduce(compatibleType(options))
              StructField(name, dataType, nullable = true)
          }
          val newFieldsArr = newFields.toArray
          java.util.Arrays.sort(newFieldsArr, structFieldComparator)
          StructType(newFieldsArr)

        case (ArrayType(elementType1, containsNull1), ArrayType(elementType2, containsNull2)) =>
          ArrayType(
            compatibleType(options)(elementType1, elementType2), containsNull1 || containsNull2)

        // In XML datasource, since StructType can be compared with ArrayType.
        // In this case, ArrayType wraps the StructType.
        case (ArrayType(ty1, _), ty2) =>
          ArrayType(compatibleType(options)(ty1, ty2))

        case (ty1, ArrayType(ty2, _)) =>
          ArrayType(compatibleType(options)(ty1, ty2))

        // As this library can infer an element with attributes as StructType whereas
        // some can be inferred as other non-structural data types, this case should be
        // treated.
        case (st: StructType, dt: DataType) if st.fieldNames.contains(options.valueTag) =>
          val valueIndex = st.fieldNames.indexOf(options.valueTag)
          val valueField = st.fields(valueIndex)
          val valueDataType = compatibleType(options)(valueField.dataType, dt)
          st.fields(valueIndex) = StructField(options.valueTag, valueDataType, nullable = true)
          st

        case (dt: DataType, st: StructType) if st.fieldNames.contains(options.valueTag) =>
          val valueIndex = st.fieldNames.indexOf(options.valueTag)
          val valueField = st.fields(valueIndex)
          val valueDataType = compatibleType(options)(dt, valueField.dataType)
          st.fields(valueIndex) = StructField(options.valueTag, valueDataType, nullable = true)
          st

        // TODO: These null type checks should be in `findTightestCommonTypeOfTwo`.
        case (_, NullType) => t1
        case (NullType, _) => t2
        // strings and every string is a XML object.
        case (_, _) => StringType
      }
    }
  }
}
