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

import scala.collection.Seq
import scala.collection.mutable.ArrayBuffer

import org.w3c.dom.Node

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import com.databricks.spark.xml.util.InferSchema


/**
 * Wraps parser to iteratoration process.
 */

private[xml] object DomXmlPartialSchemaParser {
  def parse(xml: RDD[String],
            samplingRatio: Double,
            parseMode: String,
            excludeAttributeFlag: Boolean,
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
        val conf = DomConfiguration(excludeAttributeFlag, treatEmptyValuesAsNulls)
        val parser = new DomXmlParser(childNode, conf)
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
  private def inferField(parser: DomXmlParser,
                         node: Node,
                         conf: DomConfiguration): DataType = {
    inferField(parser.inferDataType(node), parser, node, conf)
  }

  private def inferArrayEelementField(parser: DomXmlParser,
                                      node: Node,
                                      conf: DomConfiguration): DataType = {
    inferField(parser.inferArrayElementType(node), parser, node, conf)
  }

  private def inferField(dataType: Int,
                         parser: DomXmlParser,
                         node: Node,
                         conf: DomConfiguration): DataType = {
    import com.databricks.spark.xml.parsers.dom.DomXmlParser._
    dataType match {
      case INTEGER =>
        LongType

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

      case TIMESTAMP =>
        TimestampType

      case OBJECT =>
        inferObject(new DomXmlParser(node, conf))

      case ARRAY =>
        partiallyInferArray(parser, node, conf)

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
      val inferredType = inferField(parser, node, parser.getConf)
      inferredType match {
        // For XML, it can contains the same keys.
        // So we need to manually merge them to an array.
        case ArrayType(st, _) =>
          val dataTypes = partialInferredArrayTypes.getOrElse(field, ArrayBuffer.empty[DataType])
          dataTypes += st
          partialInferredArrayTypes += (field -> dataTypes)
        case _ =>
          builder += StructField(field, inferField(parser, node, parser.getConf), nullable = true)
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

  def partiallyInferArray(parser: DomXmlParser, node: Node, conf: DomConfiguration): DataType = {
    ArrayType(inferArrayEelementField(parser, node, conf))
  }
}
