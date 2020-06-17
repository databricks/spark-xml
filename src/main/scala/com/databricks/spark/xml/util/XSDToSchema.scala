/*
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

import java.io.{File, FileInputStream, InputStreamReader, StringReader}
import java.nio.charset.StandardCharsets
import java.nio.file.Path

import scala.collection.JavaConverters._
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.types._
import org.apache.ws.commons.schema._
import org.apache.ws.commons.schema.constants.Constants

/**
 * Utility to generate a Spark schema from an XSD. Not all XSD schemas are simple tabular schemas,
 * so not all elements or XSDs are supported.
 */
@Experimental
object XSDToSchema {

  /**
   * Reads a schema from an XSD file.
   *
   * @param xsdFile XSD file
   * @return Spark-compatible schema
   */
  @Experimental
  def read(xsdFile: File): StructType = {
    val xmlSchema = new XmlSchemaCollection().read(
      new InputStreamReader(new FileInputStream(xsdFile), StandardCharsets.UTF_8))
    getStructType(xmlSchema)
  }

  /**
   * Reads a schema from an XSD file.
   *
   * @param xsdFile XSD file
   * @return Spark-compatible schema
   */
  @Experimental
  def read(xsdFile: Path): StructType = read(xsdFile.toFile)

  /**
   * Reads a schema from an XSD as a string.
   *
   * @param xsdString XSD as a string
   * @return Spark-compatible schema
   */
  @Experimental
  def read(xsdString: String): StructType = {
    val xmlSchema = new XmlSchemaCollection().read(new StringReader(xsdString))
    getStructType(xmlSchema)
  }


  private def getStructField(xmlSchema: XmlSchema, schemaType: XmlSchemaType): StructField = {
    schemaType match {
      // xs:simpleType
      case simpleType: XmlSchemaSimpleType =>
        val schemaType = simpleType.getContent match {
          case restriction: XmlSchemaSimpleTypeRestriction =>
            val matchType =
              if (restriction.getBaseTypeName == Constants.XSD_ANYSIMPLETYPE) {
                simpleType.getQName
              } else {
                restriction.getBaseTypeName
              }
            matchType match {
              case Constants.XSD_BOOLEAN => BooleanType
              case Constants.XSD_DECIMAL =>
                val scale = restriction.getFacets.asScala.collectFirst {
                  case facet: XmlSchemaFractionDigitsFacet => facet
                }
                scale match {
                  case Some(scale) => DecimalType(38, scale.getValue.toString.toInt)
                  case None => DecimalType(38, 18)
                }
              case Constants.XSD_UNSIGNEDLONG => DecimalType(38, 0)
              case Constants.XSD_DOUBLE => DoubleType
              case Constants.XSD_FLOAT => FloatType
              case Constants.XSD_BYTE => ByteType
              case Constants.XSD_SHORT |
                   Constants.XSD_UNSIGNEDBYTE => ShortType
              case Constants.XSD_INTEGER |
                   Constants.XSD_NEGATIVEINTEGER |
                   Constants.XSD_NONNEGATIVEINTEGER |
                   Constants.XSD_NONPOSITIVEINTEGER |
                   Constants.XSD_POSITIVEINTEGER |
                   Constants.XSD_UNSIGNEDSHORT => IntegerType
              case Constants.XSD_LONG |
                   Constants.XSD_UNSIGNEDINT => LongType
              case _ => StringType
            }
          case _ => StringType
        }
        StructField("baseName", schemaType)

      // xs:complexType
      case complexType: XmlSchemaComplexType =>
        complexType.getContentModel match {
          case content: XmlSchemaSimpleContent =>
            // xs:simpleContent
            content.getContent match {
              case extension: XmlSchemaSimpleContentExtension =>
                val baseStructField = getStructField(xmlSchema,
                  xmlSchema.getTypeByName(extension.getBaseTypeName))
                val value = StructField("_VALUE", baseStructField.dataType)
                val attributes = extension.getAttributes.asScala.map {
                  case attribute: XmlSchemaAttribute =>
                    val baseStructField = getStructField(xmlSchema,
                      xmlSchema.getTypeByName(attribute.getSchemaTypeName))
                    StructField(s"_${attribute.getName}", baseStructField.dataType)
                }
                StructField(complexType.getName, StructType(value +: attributes))
            }
          case null =>
            complexType.getParticle match {
              // xs:all
              case all: XmlSchemaAll =>
                val fields = all.getItems.asScala.map {
                  case element: XmlSchemaElement =>
                    val baseStructField = getStructField(xmlSchema, element.getSchemaType)
                    val field = StructField(element.getName, baseStructField.dataType)
                    if (element.getMaxOccurs == 1) {
                      field
                    } else {
                      StructField(element.getName, ArrayType(field.dataType))
                    }
                }
                StructField(complexType.getName, StructType(fields))
              // xs:choice
              case choice: XmlSchemaChoice =>
                val fields = choice.getItems.asScala.map { case element: XmlSchemaElement =>
                  val baseStructField = getStructField(xmlSchema, element.getSchemaType)
                  val field = StructField(element.getName, baseStructField.dataType)
                  if (element.getMaxOccurs == 1) {
                    field
                  } else {
                    StructField(element.getName, ArrayType(field.dataType))
                  }
                }
                StructField(complexType.getName, StructType(fields))
              // xs:sequence
              case sequence: XmlSchemaSequence =>
                // flatten xs:choice nodes
                val fields = sequence.getItems.asScala.flatMap { member: XmlSchemaSequenceMember =>
                    member match {
                      case choice: XmlSchemaChoice =>
                        choice.getItems.asScala.map(e => (e.asInstanceOf[XmlSchemaElement], true))
                      case element: XmlSchemaElement => Seq((element, element.getMinOccurs == 0))
                    }
                  }.map { case (element: XmlSchemaElement, nullable) =>
                    val baseStructField = getStructField(xmlSchema, element.getSchemaType)
                    val field = StructField(element.getName, baseStructField.dataType, nullable)
                    if (element.getMaxOccurs == 1) {
                      field
                    } else {
                      StructField(element.getName, ArrayType(field.dataType))
                    }
                  }
                StructField(complexType.getName, StructType(fields))
            }
        }
    }
  }

  private def getStructType(xmlSchema: XmlSchema): StructType = {
    val (qName, schemaElement) = xmlSchema.getElements.asScala.head
    val schemaType = schemaElement.getSchemaType
    if (schemaType.isAnonymous) {
      schemaType.setName(qName.getLocalPart)
    }
    StructType(Seq(getStructField(xmlSchema, schemaType)))
  }

}
