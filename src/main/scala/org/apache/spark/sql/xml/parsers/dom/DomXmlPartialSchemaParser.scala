/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.xml.parsers.dom

import java.io.ByteArrayInputStream
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.stream.XMLInputFactory

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.xml.util.InferSchema
import org.w3c.dom.Node

import scala.collection.Seq
import scala.collection.mutable.ArrayBuffer

/**
 * Wraps parser to iteratoration process.
 */

private[sql] object DomXmlPartialSchemaParser {
  def apply(xml: RDD[String],
            rootTag: String,
            samplingRatio: Double,
            parseMode: String,
            includeAttributeFlag: Boolean,
            treatEmptyValuesAsNulls: Boolean): RDD[DataType] = {
    require(samplingRatio > 0, s"samplingRatio ($samplingRatio) should be greater than 0")
    val schemaData = if (samplingRatio > 0.99) {
      xml
    } else {
      xml.sample(withReplacement = false, samplingRatio, 1)
    }

    schemaData.mapPartitions { iter =>
      iter.flatMap { xml =>
        val builder = DocumentBuilderFactory.newInstance().newDocumentBuilder()

        // It does not have to skip for white space, since [[XmlInputFormat]]
        // always finds the root tag without a heading space.
        val childNode = builder.parse(new ByteArrayInputStream(xml.getBytes))
          .getChildNodes.item(0)
        val parser = new DomXmlParser(childNode)
        if (parser.isEmpty) {
          None
        } else {
          Some(inferObject(parser))
        }
      }
    }
  }

  /**
   * Infer the type of a xml document from the parser's token stream
   */
  private def inferField(parser: DomXmlParser, node: Node): DataType = {
    inferField(parser.inferDataType(node), parser, node)
  }

  private def inferArrayEelementField(parser: DomXmlParser, node: Node): DataType = {
    inferField(parser.inferArrayElementType(node), parser, node)
  }

  private def inferField(dataType: Int, parser: DomXmlParser, node: Node): DataType = {
    import org.apache.spark.sql.xml.parsers.dom.DomXmlParser._
    dataType match {
      case LONG =>
        LongType

      case DOUBLE =>
        DoubleType

      case BOOLEAN =>
        BooleanType

      case STRING =>
        StringType

      case NULL =>
        NullType

      case OBJECT =>
        inferObject(new DomXmlParser(node))

      case ARRAY =>
        partiallyInferArray(parser, node)

      case _ =>
      // TODO: Now it skips unsupported types (we might have to treat null values).
        StringType
    }
  }

  def inferObject(parser: DomXmlParser): DataType = {
    val builder = Seq.newBuilder[StructField]
    val partialInferredArrayTypes = collection.mutable.Map[String, ArrayBuffer[DataType]]()
    parser.foreach{ node =>
      val field = node.getNodeName
      val inferredType = inferField(parser, node)
      inferredType match {
        // For XML, it can contains the same keys.
        // So we need to manually merge them to an array.
        case ArrayType(st, _) =>
          val dataTypes = partialInferredArrayTypes.getOrElse(field, ArrayBuffer.empty[DataType])
          dataTypes += st
          partialInferredArrayTypes += (field -> dataTypes)
        case _ =>
          builder += StructField(field, inferField(parser, node), nullable = true)
      }
    }

    // We need to manually merges all the [[ArrayType]]s.
    partialInferredArrayTypes.foreach{
      case (field, dataTypes) =>
        val elementType = dataTypes.reduceLeft(InferSchema.compatibleType)
        builder += StructField(field, ArrayType(elementType), nullable = true)
    }

    StructType(builder.result().sortBy(_.name))
  }

  def partiallyInferArray(parser: DomXmlParser, node: Node): DataType = {

    // If this XML array is empty, we use NullType as a placeholder.
    // If this array is not empty in other XML objects, we can resolve
    // the type as we pass through all XML objects.
    var elementType: DataType = NullType
    elementType = InferSchema.compatibleType(elementType, inferArrayEelementField(parser, node))
    ArrayType(elementType)
  }
}
