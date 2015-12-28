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
package com.databricks.spark.xml.parsers.dom

import java.io.ByteArrayInputStream
import javax.xml.parsers.DocumentBuilderFactory

import scala.collection.mutable.ArrayBuffer

import org.xml.sax.SAXException
import org.slf4j.LoggerFactory
import org.w3c.dom.Node

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import com.databricks.spark.xml.util.TypeCast._
import com.databricks.spark.xml.parsers.dom.DomXmlParser._

/**
 *  Configuration used during parsing.
 */
private[xml] case class DomConfiguration(excludeAttributeFlag: Boolean = false,
                                         treatEmptyValuesAsNulls: Boolean = false,
                                         failFastFlag: Boolean = false)

private[xml] class DomXmlParser(doc: Node, conf: DomConfiguration = DomConfiguration())
    extends Iterable[Node] {

  lazy val nodes = readChildNodes
  var index: Int = 0

  override def iterator: Iterator[Node] = nodes.iterator
  override def isEmpty: Boolean = nodes.isEmpty

  /**
   * Read all the event to infer datatypes.
   */
  private def readChildNodes: Array[Node] = {

    // Add all the nodes including attributes.
    val nodesBuffer: ArrayBuffer[Node] = ArrayBuffer.empty[Node]

    if (doc.hasChildNodes) {
      val childNodes = doc.getChildNodes
      nodesBuffer ++= (0 until childNodes.getLength).map(childNodes.item)
    }

    if (!conf.excludeAttributeFlag && doc.hasAttributes) {
      val attributeNodes = doc.getAttributes
      nodesBuffer ++= (0 until attributeNodes.getLength).map(attributeNodes.item)
    }

    // Filter the white spaces between tags.
    nodesBuffer.filterNot(_.getNodeType == Node.TEXT_NODE).toArray
  }

  /**
   * This infers the type of data.
   */
  def inferDataType(node: Node): Int = {
    // If there are an element having the same name, then it is an Array.
    if (nodes.count(_.getNodeName == node.getNodeName) > 1) {
      ARRAY
    } else if (checkObjectType(node)) {
      OBJECT
    } else {
      inferPrimitiveType(node)
    }
  }

  private def checkObjectType(node: Node): Boolean = {
    lazy val isObject = (0 until node.getChildNodes.getLength)
      .map(node.getChildNodes.item)
      .exists(_.getNodeType == Node.ELEMENT_NODE)
    isObject
  }

  /**
   * !! HACK ALERT !!
   *
   * This function is separately made. Because `inferDataType` goes infinite loop
   * after the current node is inferred as ARRAY type.
   * So, after checking that, we need to not infer schema of the elements of that ARRAY.
   * It avoids this by calling `checkObjectType` and `inferPrimitiveType` instread of
   * `inferDataType` at [[DomXmlPartialSchemaParser]].
   */
  def inferArrayElementType(node: Node): Int = {
    if (checkObjectType(node)){
      OBJECT
    } else {
      inferPrimitiveType(node)
    }
  }

  private def inferPrimitiveType(node: Node): Int = {
    val data = node.getTextContent
    if (data.trim.length <= 0) {
      if (conf.treatEmptyValuesAsNulls) {
        NULL
      } else {
        STRING
      }
    } else if (data == null) {
      NULL
    } else if (isLong(data)) {
      INTEGER
    } else if (isLong(data)) {
      LONG
    } else if (isDouble(data)) {
      DOUBLE
    } else if (isBoolean(data)) {
      BOOLEAN
    } else if (isTimestamp(data)){
      TIMESTAMP
    } else {
      STRING
    }
  }

  def getConf: DomConfiguration = {
    conf
  }
}


/**
 * Wraps parser to iteratoration process.
 */
private[xml] object DomXmlParser {

  private val logger = LoggerFactory.getLogger(DomXmlParser.getClass)

  /**
   * This defines the possible types for XML.
   */
  val FAIL: Int = -1
  val NULL: Int = 1
  val BOOLEAN: Int = 2
  val INTEGER: Int = 3
  val LONG: Int = 4
  val DOUBLE: Int = 5
  val STRING: Int = 6
  val TIMESTAMP: Int = 7
  val OBJECT: Int = 8
  val ARRAY: Int = 9

  def parse(xml: RDD[String],
            schema: StructType,
            conf: DomConfiguration): RDD[Row] = {
    val failFast = conf.failFastFlag

    xml.mapPartitions { iter =>
      iter.flatMap { xml =>
        // It does not have to skip for white space, since [[XmlInputFormat]]
        // always finds the root tag without a heading space.
        val builder = DocumentBuilderFactory.newInstance().newDocumentBuilder()

        // We treats the exception explicitly below. So turn off the error handler.
        builder.setErrorHandler(null)
        try{
          val childNode = builder.parse(new ByteArrayInputStream(xml.getBytes))
            .getChildNodes.item(0)
          val parser = new DomXmlParser(childNode, conf)
          if (parser.isEmpty) {
            None
          }
          Some(convertObject(parser, schema))
        } catch {
          // Java library DOMParser throws SAXException when it fails.
          case se: SAXException if !failFast =>
            logger.warn(s"Dropping malformed row: ${xml.replaceAll("\n", "")}")
            None
          case se: SAXException if failFast =>
            throw new RuntimeException(s"Malformed row (failing fast): ${xml.replaceAll("\n", "")}")
        }
      }
    }
  }

  /**
   * Parse the current token (and related children) according to a desired schema
   */

  private[xml] def convertField(node: Node,
                                dataType: DataType,
                                conf: DomConfiguration ): Any = {
    val shouldBeNullValue = node.getTextContent == null ||
      node.getTextContent.size <= 0 && conf.treatEmptyValuesAsNulls

    if (shouldBeNullValue) {
      null
    }
    else {
      dataType match {
        case LongType =>
          signSafeToLong(node.getTextContent)

        case DoubleType =>
          signSafeToDouble(node.getTextContent)

        case BooleanType =>
          castTo(node.getTextContent, BooleanType)

        case StringType =>
          castTo(node.getTextContent, StringType)

        case DateType =>
          castTo(node.getTextContent, DateType)

        case TimestampType =>
          castTo(node.getTextContent, TimestampType)

        case FloatType =>
          signSafeToFloat(node.getTextContent)

        case ByteType =>
          castTo(node.getTextContent, ByteType)

        case ShortType =>
          castTo(node.getTextContent, ShortType)

        case IntegerType =>
          signSafeToInt(node.getTextContent)

        case dt: DecimalType =>
          castTo(node.getTextContent, new DecimalType(None))

        case NullType =>
          null

        case ArrayType(st, _) =>
          convertField(node, st, conf)

        case MapType(StringType, vt, _) =>
          val nestedParser = new DomXmlParser(node, conf)
          convertMap(nestedParser, vt)

        case st: StructType =>
          val nestedParser = new DomXmlParser(node, conf)
          convertObject(nestedParser, st)

        case (udt: UserDefinedType[_]) =>
          convertField(node, udt.sqlType, conf)

        case dataType =>
          sys.error(s"Failed to parse a value for data type $dataType.")
      }
    }
  }

  /**
   * Parse an object as map.
   */
  private def convertMap(parser: DomXmlParser,
                            valueType: DataType): Map[String, Any] = {
    val keys = ArrayBuffer.empty[String]
    val values = ArrayBuffer.empty[Any]
    parser.foreach { node =>
      keys += node.getNodeName
      values += convertField(node, valueType, parser.getConf)
    }
    keys.zip(values).toMap
  }

  /**
   * Parse an object from the token stream into a new Row representing the schema.
   * Fields in the xml that are not defined in the requested schema will be dropped.
   */
  private def convertObject(parser: DomXmlParser,
                            schema: StructType): Row = {
    val row = new Array[Any](schema.length)
    parser.foreach{ node =>
      val field = node.getNodeName
      val nameToIndex = schema.map(_.name).zipWithIndex.toMap
      nameToIndex.get(field) match {
        case Some(index) =>
          val dataType = schema(index).dataType
          dataType match {
            // For XML, it can contains the same keys. So we need to manually merge them
            // to an array. ArrayType is always wrapped with StructType. So, it is safe
            // to convert here.
            case ArrayType(st, _) =>
              val elements = {
                val values = Option(row(index))
                  .map(_.asInstanceOf[ArrayBuffer[Any]])
                  .getOrElse(ArrayBuffer.empty[Any])
                val newValue = convertField(node, dataType, parser.getConf)
                values :+ newValue
              }
              row(index) = elements
            case _ =>
              row(index) = convertField(node, dataType, parser.getConf)
          }
        case _ =>
          // This case must not happen.
          throw new IndexOutOfBoundsException(s"The field ('$field') does not exist in schema")
      }
    }
    Row.fromSeq(row)
  }
}
