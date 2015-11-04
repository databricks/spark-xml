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

import javax.xml.stream.events.Attribute
import javax.xml.stream.XMLInputFactory

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.xml.parsers.io.BaseRDDInputStream
import org.apache.spark.sql.xml.util.InferSchema

import scala.collection.Seq

/**
 * Wraps parser to iteratoration process.
 */
private[sql] object StaxXmlPartialSchemaRDD {
  def apply(xml: RDD[String], samplingCount: Int)
           (sqlContext: SQLContext): RDD[DataType] = {
    val factory = XMLInputFactory.newInstance()
    val parser = new StaxXmlParser(factory.createXMLEventReader(new BaseRDDInputStream(xml)))
    val stream = this(parser, samplingCount)
    sqlContext.sparkContext.parallelize[DataType](stream)
  }

  private def apply(parser: StaxXmlParser, samplingCount: Int): Stream[DataType] = {

    new Iterator[DataType] {
      var count = 0
      var maybePartialSchemas: Option[DataType] = None

      override def hasNext: Boolean = {
        if (maybePartialSchemas.isDefined) {
          true
        } else {
          // TODO: The root should be given from users as an option.
          maybePartialSchemas = startInferField(parser)
          count < samplingCount && maybePartialSchemas.isDefined
        }
      }

      override def next: DataType = {
        count += count
        val partialSchemas = maybePartialSchemas.orNull
        maybePartialSchemas = None
        partialSchemas
      }
    }.toStream
  }

  /**
   * Infer the type of a xml document from the parser's token stream
   */

  private def startInferField(parser: StaxXmlParser): Option[DataType] = {
    if (parser.readFragment()) {
      val field = parser.nextEvent.asStartElement.getName.getLocalPart
      Some(inferObject(parser, field))
    } else {
      None
    }
  }

  private def inferField(parser: StaxXmlParser, parentField: String): DataType = {
    import org.apache.spark.sql.xml.parsers.stax.XmlDataTypes._
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
      //TODO: Now it skips unsupported types (we might have to treat null values).
        StringType
    }
  }

  def inferObject(parser: StaxXmlParser, parentField: String): DataType = {
    val builder = Seq.newBuilder[StructField]
    while (parser.skipEndElementUntil(parentField)) {
      val event = parser.nextEvent
      // Type is not infered for attributes.
      if (event.isAttribute) {
        val field = event.asInstanceOf[Attribute].getName.getLocalPart
        builder += StructField(field, StringType, nullable = true)
      } else {
        val field = if (event.isStartElement) {
          event.asStartElement.getName.getLocalPart
        } else {
          // This case should not happen since values are covered for other cases
          // and end element is skipped by `skipEndElementUntil()`.

          // TODO: When the value is only the child of the document, it comes to this case.
          "null"
        }
        builder += StructField(field, inferField(parser, field), nullable = true)
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

      // Type is not infered for attributes.
      if (event.isAttribute) {
        val field = event.asInstanceOf[Attribute].getName.getLocalPart
        elementType = StringType
      }

      else {
        val field = if (event.isStartElement) {
          event.asStartElement.getName.getLocalPart
        } else {
          // This case should not happen since values are covered for other cases
          // and end element is skipped by `skipEndElementUntil()`.
          "null"
        }
        elementType = InferSchema.compatibleType(elementType, inferField(parser, field))
      }
    }

    ArrayType(elementType)
  }
}
