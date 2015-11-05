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
package org.apache.spark.sql.xml.parsers.stax

import java.io.ByteArrayInputStream
import javax.xml.stream.XMLInputFactory

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.xml.util.InferSchema

import scala.collection.Seq

/**
 * Wraps parser to iteratoration process.
 */
private[sql] object StaxXmlPartialSchemaParser {
  def apply(xml: RDD[String], rootTag: String, samplingRatio: Double)
           (sqlContext: SQLContext): RDD[DataType] = {
    require(samplingRatio > 0, s"samplingRatio ($samplingRatio) should be greater than 0")
    val schemaData = if (samplingRatio > 0.99) {
      xml
    } else {
      xml.sample(withReplacement = false, samplingRatio, 1)
    }

    schemaData.mapPartitions { iter =>
      iter.flatMap { xml =>
        val factory = XMLInputFactory.newInstance()
        val parser = new StaxXmlParser(
          factory.createXMLEventReader(new ByteArrayInputStream(xml.getBytes)))
        startInferField(parser, rootTag)
      }
    }
  }

  /**
   * Infer the type of a xml document from the parser's token stream
   */

  private def startInferField(parser: StaxXmlParser, rootTag: String): Option[DataType] = {
    if (parser.readAllEventsInFragment) {
      // Skip the first event
      parser.nextEvent
      Some(inferObject(parser, rootTag))
    } else {
      None
    }
  }

  private def inferField(parser: StaxXmlParser, parentField: String): DataType = {
    import org.apache.spark.sql.xml.parsers.stax.StaxXmlParser._
    parser.inferDataType match {
      case LONG =>
        parser.nextEvent
        LongType

      case DOUBLE =>
        parser.nextEvent
        DoubleType

      case BOOLEAN =>
        parser.nextEvent
        BooleanType

      case STRING =>
        parser.nextEvent
        StringType

      case NULL =>
        parser.nextEvent
        NullType

      case OBJECT =>
        inferObject(parser, parentField)

      case ARRAY =>
        inferArray(parser, parentField)

      case _ =>
      // TODO: Now it skips unsupported types (we might have to treat null values).
        StringType
    }
  }

  def inferObject(parser: StaxXmlParser, parentField: String): DataType = {
    val builder = Seq.newBuilder[StructField]
    while (parser.skipEndElementUntil(parentField)) {
      val event = parser.nextEvent
      if (event.isStartElement) {
        val field = event.asStartElement.getName.getLocalPart
        builder += StructField(field, inferField(parser, field), nullable = true)
      } else if (event.isEndElement) {

        // In this case, the given element does not have any value.
        val field = event.asEndElement.getName.getLocalPart
        builder += StructField(field, NullType, nullable = true)
      } else {

        // This case should not happen since values are covered for other cases
        // and end element is skipped by `skipEndElementUntil()`.
        // TODO: When the value is only the child of the document, it comes to this case.
        throw new RuntimeException("Given element type is not StartEelement or EndEelementEvent")
      }
    }

    StructType(builder.result().sortBy(_.name))
  }

  def inferArray(parser: StaxXmlParser, parentField: String): DataType = {

    // If this XML array is empty, we use NullType as a placeholder.
    // If this array is not empty in other XML objects, we can resolve
    // the type as we pass through all XML objects.
    var elementType: DataType = NullType
    while (parser.skipEndElementUntil(parentField)) {
      val event = parser.nextEvent
      if (event.isStartElement) {
        val field = event.asStartElement.getName.getLocalPart
        elementType = InferSchema.compatibleType(elementType, inferField(parser, field))
      } else if (event.isEndElement) {

        // In this case, the given element does not have any value.
        val field = event.asEndElement.getName.getLocalPart
        elementType = NullType
      } else if (event.isCharacters){

        // In array, this can have charater event since it treats just as an element in XML
        elementType = InferSchema.compatibleType(elementType, inferField(parser, null))
      } else {

        // This case should not happen since values are covered for other cases
        // and end element is skipped by `skipEndElementUntil()`.
        // TODO: When the value is only the child of the document, it comes to this case.
        throw new RuntimeException("Given element type is not StartEelement or EndEelementEvent")
      }

    }

    ArrayType(elementType)
  }
}
