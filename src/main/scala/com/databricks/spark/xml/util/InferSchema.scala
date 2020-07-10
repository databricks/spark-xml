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

import com.databricks.spark.xml.parsers.StaxXmlParserUtils
import com.databricks.spark.xml.parsers.StaxXmlParserUtils.{ convertAttributesToValuesMap, skipUntil }
import com.databricks.spark.xml.util.TypeCast._
import com.databricks.spark.xml.{ XmlOptions, XmlPath }
import javax.xml.stream.events.{ Attribute, Characters, EndDocument, EndElement, StartElement, XMLEvent }
import javax.xml.stream.{ XMLEventReader, XMLStreamConstants }
import javax.xml.transform.stream.StreamSource
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.collection.Seq
import scala.util.control.NonFatal

private[xml] object InferSchema {

  /**
   * Copied from internal Spark api
   * [[org.apache.spark.sql.catalyst.analysis.TypeCoercion]]
   */
  private[this] val numericPrecedence: IndexedSeq[DataType] =
    IndexedSeq[DataType](
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType,
      TimestampType,
      DecimalType.SYSTEM_DEFAULT)

  private[this] val findTightestCommonTypeOfTwo: (DataType, DataType) => Option[DataType] = {
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
    implicit val xmlOptions = options
    val schemaData = if (options.samplingRatio > 0.99) {
      xml
    } else {
      xml.sample(withReplacement = false, options.samplingRatio, 1)
    }
    // perform schema inference on each row and merge afterwards
    val rootType = schemaData.mapPartitions { iter =>
      val xsdSchema = Option(options.rowValidationXSDPath).map(ValidatorUtil.getSchema)

      iter.flatMap { xml =>
        try {
          xsdSchema.foreach { schema =>
            schema.newValidator().validate(new StreamSource(new StringReader(xml)))
          }

          val parser = StaxXmlParserUtils.filteredReader(xml)
          Some(inferObject(parser))
        } catch {
          case NonFatal(e) if options.parseMode == PermissiveMode =>
            Some(StructType(Seq(StructField(xmlOptions.columnNameOfCorruptRecord, StringType))))
          case NonFatal(_) =>
            None
        }
      }
    }.fold(StructType(Seq()))(compatibleType)

    canonicalizeType(rootType) match {
      case Some(st: StructType) => st
      case _ =>
        // canonicalizeType erases all empty structs, including the only one we want to keep
        StructType(Seq())
    }
  }

  private[this] def inferFrom(datum: String)(implicit options: XmlOptions): DataType = {
    val value = if (datum != null && options.ignoreSurroundingSpaces) {
      datum.trim()
    } else {
      datum
    }

    if (options.inferSchema) {
      value match {
        case null => NullType
        case v if v.isEmpty => NullType
        case v if !options.zeroPrefixedStringsAsNumber
          && v.length > 1
          && v.headOption.contains('0') => StringType
        case v if isLong(v) => LongType
        case v if isInteger(v) => IntegerType
        case v if isDouble(v) => DoubleType
        case v if isBoolean(v) => BooleanType
        case v if isTimestamp(v) => TimestampType
        case _ => StringType
      }
    } else {
      StringType
    }
  }

  private[this] def inferObject(parser: XMLEventReader)(implicit xmlOptions: XmlOptions): DataType = {

    def getAttributeDataTypes(startElement: StartElement): Seq[StructField] = {
      val attributes = startElement.getAttributes.asScala.collect { case a: Attribute => a }.toArray

      convertAttributesToValuesMap(attributes, xmlOptions)
        .mapValues(inferFrom)
        .map { case (name, dataType) => StructField(name, dataType) }
        .toSeq
    }

    def collapseSingleValueStructs(dataType: DataType): DataType = {
      val valueTag = xmlOptions.valueTag
      dataType match {
        case StructType(fields) if fields.isEmpty && xmlOptions.nullValue != null => inferFrom(xmlOptions.nullValue)
        case StructType(Array(StructField(`valueTag`, dataType, _, _))) => dataType
        case StructType(fields) => StructType(fields.map(field => field.copy(dataType = collapseSingleValueStructs(field.dataType))))
        case ArrayType(elementType, _) => ArrayType(collapseSingleValueStructs(elementType))
        case nonStructDataType: DataType => nonStructDataType
      }
    }

    @scala.annotation.tailrec
    def processEvent(currentPath: XmlPath, nextEvent: XMLEvent, inProgressSchema: XmlElementTypes): DataType = {
      nextEvent match {
        case startElement: StartElement =>
          val fields = getAttributeDataTypes(startElement)
          val currentElementPath = currentPath.child(startElement.getName.getLocalPart)
          processEvent(currentElementPath, parser.nextEvent(), inProgressSchema.addValue(currentElementPath, StructType(fields)))

        case characters: Characters if !characters.isWhiteSpace =>
          processEvent(currentPath, parser.nextEvent(),
            inProgressSchema.appendToType(currentPath, xmlOptions.valueTag, inferFrom(characters.getData)))

        case end: EndElement if end.getName.getLocalPart != currentPath.leafName =>
          throw new IllegalStateException(s"Expected closing element for $currentPath but found ${end.getName.getLocalPart}")

        case _: EndElement =>
          val mergedSchema = inProgressSchema.children(currentPath).foldLeft(inProgressSchema) {
            case (runningSchema, (name, dataType +: Nil)) => runningSchema.appendToType(currentPath, name, dataType)
            case (runningSchema, (name, dataTypes)) => runningSchema.appendToType(currentPath, name, ArrayType(dataTypes.reduceLeft(compatibleType(_, _))))
          }.dropChildren(currentPath)

          val withEmptyAsNull = mergedSchema.currentType(currentPath) match {
            case StructType(fields) if xmlOptions.treatEmptyValuesAsNulls && fields.isEmpty => mergedSchema.updateCurrentType(currentPath, NullType)
            case _ => mergedSchema
          }

          if (currentPath.parent.isRoot) {
            withEmptyAsNull.currentType(currentPath)
          } else {
            processEvent(currentPath.parent, parser.nextEvent(), withEmptyAsNull)
          }
        case _: EndDocument =>
          inProgressSchema.aggregatedType
        case _ =>
          processEvent(currentPath, parser.nextEvent(), inProgressSchema)
      }
    }

    val rootEvent = skipUntil(parser, XMLStreamConstants.START_ELEMENT).asStartElement()
    val detectedSchema = processEvent(
      currentPath = XmlPath.root,
      nextEvent = rootEvent,
      inProgressSchema = new XmlElementTypes()(xmlOptions))
    collapseSingleValueStructs(detectedSchema)
  }

  /**
   * Convert NullType to StringType and remove StructTypes with no fields
   */
  private[this] def canonicalizeType(dt: DataType): Option[DataType] = dt match {
    case at@ArrayType(elementType, _) =>
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
  private[this] def compatibleType(t1: DataType, t2: DataType)(implicit options: XmlOptions): DataType = {
    def matchStructAndValueTag(struct: StructType, dataType: DataType) = {
      val valueIndex = struct.fieldNames.indexOf(options.valueTag)
      val valueField = struct.fields(valueIndex)
      val valueDataType = compatibleType(valueField.dataType, dataType)
      struct.fields(valueIndex) = StructField(options.valueTag, valueDataType, nullable = true)
      struct
    }

    // TODO: Optimise this logic.
    val resultType = findTightestCommonTypeOfTwo(t1, t2).getOrElse {
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
              val dataType = fieldTypes.view.map(_.dataType).reduce(compatibleType(_, _))
              StructField(name, dataType, nullable = true)
          }
          val newFieldsArr = newFields.toArray
          java.util.Arrays.sort(newFieldsArr, structFieldComparator)
          StructType(newFieldsArr)

        case (ArrayType(elementType1, containsNull1), ArrayType(elementType2, containsNull2)) =>
          ArrayType(
            compatibleType(elementType1, elementType2), containsNull1 || containsNull2)

        // In XML datasource, since StructType can be compared with ArrayType.
        // In this case, ArrayType wraps the StructType.
        case (ArrayType(ty1, _), ty2) =>
          ArrayType(compatibleType(ty1, ty2))

        case (ty1, ArrayType(ty2, _)) =>
          ArrayType(compatibleType(ty1, ty2))

        // As this library can infer an element with attributes as StructType whereas
        // some can be inferred as other non-structural data types, this case should be
        // treated.
        case (st: StructType, dt: DataType) if st.fieldNames.contains(options.valueTag) => matchStructAndValueTag(st, dt)
        case (dt: DataType, st: StructType) if st.fieldNames.contains(options.valueTag) => matchStructAndValueTag(st, dt)

        case (st: StructType, dt: DataType) => StructType(st.fields :+ StructField(options.valueTag, dt))
        case (dt: DataType, st: StructType) => StructType(st.fields :+ StructField(options.valueTag, dt))

        // TODO: These null type checks should be in `findTightestCommonTypeOfTwo`.
        case (_, NullType) => t1
        case (NullType, _) => t2
        // strings and every string is a XML object.
        case (_, _) => StringType
      }
    }
    resultType
  }

  class XmlElementTypes(detectedDataTypes: Map[XmlPath, Seq[DataType]] = Map.empty[XmlPath, Seq[DataType]]
                       )(implicit xmlOptions: XmlOptions) {

    def addValue(key: XmlPath, value: DataType): XmlElementTypes = detectedDataTypes.get(key) match {
      case Some(seq) => new XmlElementTypes(detectedDataTypes + (key -> (value +: seq)))
      case None => new XmlElementTypes(detectedDataTypes + (key -> Seq(value)))
    }

    def appendToType(key: XmlPath, name: String, dataType: DataType): XmlElementTypes = detectedDataTypes.get(key) match {
      case Some((current@StructType(fields)) +: remaining) if name == xmlOptions.valueTag =>
        val mergedType = fields.find(_.name == xmlOptions.valueTag) match {
          case None => current.add(name, dataType)
          case Some(StructField(_, ArrayType(arrayDataType, _), _, _)) =>
            StructType(current.filterNot(_.name == xmlOptions.valueTag))
              .add(xmlOptions.valueTag, ArrayType(compatibleType(arrayDataType, dataType)))
          case Some(field) =>
            StructType(current.filterNot(_.name == xmlOptions.valueTag))
              .add(xmlOptions.valueTag, ArrayType(compatibleType(field.dataType, dataType)))
        }
        new XmlElementTypes(detectedDataTypes + (key -> (mergedType +: remaining)))

      case Some((current: StructType) +: remaining) =>
        new XmlElementTypes(detectedDataTypes + (key -> (current.add(name, dataType) +: remaining)))
      case Some((current: ArrayType) +: remaining) =>
        new XmlElementTypes(detectedDataTypes + (key -> (StructType(Seq(StructField(xmlOptions.valueTag, current), StructField(name, dataType))) +: remaining)))
      case Some(current +: remaining) if name == xmlOptions.valueTag =>
        new XmlElementTypes(detectedDataTypes + (key -> (ArrayType(compatibleType(current, dataType)) +: remaining)))
      case Some((_: NullType) +: remaining) =>
        new XmlElementTypes(detectedDataTypes + (key -> (StructType(Seq(StructField(name, dataType))) +: remaining)))
      case Some(current +: _) =>
        throw new IllegalStateException(s"Didn't expect to see a value data type $current for $key")
      case None => throw new IllegalStateException(s"No type recorded yet for $key")
    }

    def currentType(key: XmlPath): DataType = detectedDataTypes.get(key) match {
      case Some(current +: _) => current
      case None => throw new IllegalStateException(s"Expected to find $key in ${asString()}")
    }

    def updateCurrentType(key: XmlPath, updatedType: DataType): XmlElementTypes = detectedDataTypes.get(key) match {
      case Some(_ +: remaining) => new XmlElementTypes(detectedDataTypes + (key -> (updatedType +: remaining)))
      case None => throw new IllegalStateException(s"Expected to find $key in ${asString()}")
    }

    def children(path: XmlPath): Map[String, Seq[DataType]] = {
      detectedDataTypes.filterKeys(_.isChildOf(path)).map {
        case (key, dataTypes) => key.leafName -> dataTypes
      }
    }

    def dropChildren(parent: XmlPath): XmlElementTypes = {
      new XmlElementTypes(detectedDataTypes.filterNot { case (path, _) => path.isChildOf(parent) })
    }

    def aggregatedType: DataType = detectedDataTypes.toSeq match {
      case (_, dataType +: Nil) +: Nil => dataType
      case _ => throw new IllegalStateException(s"Types are not yet aggregated in ${asString()}")
    }

    def asString(): String = {
      detectedDataTypes.map {
        case (key, value +: Nil) => s"$key -> $value"
        case (key, values) => s"$key -> ${values.mkString("[\n\t\t", "\n\t\t", "]")}"
      }.mkString("\n\t", "\n\t", "")
    }
  }

}
