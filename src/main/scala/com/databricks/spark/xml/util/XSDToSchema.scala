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
  val schemaNameToStructFieldMap = collection.mutable.Map[String, StructField]()

  /**
    * Reads a schema from an XSD file.
    *
    * @param xsdFile XSD file
    * @return Spark-compatible schema
    */
  @Experimental
  def read(xsdFile: File): StructType = {
    val xmlSchemaCollection = new XmlSchemaCollection()
    xmlSchemaCollection.setBaseUri(xsdFile.getParent)
    val xmlSchema = xmlSchemaCollection.read(
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
        val schemaType1 = simpleType.getContent match {
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
              case Constants.XSD_DATE => DateType
              case Constants.XSD_DATETIME => TimestampType
              case _ => StringType
            }
          case _ => StringType
        }
        schemaNameToStructFieldMap += (schemaType.getName ->
          StructField(schemaType.getName, schemaType1))
        StructField(schemaType.getName, schemaType1)

      // xs:complexType
      case complexType: XmlSchemaComplexType =>
        complexType.getContentModel match {
          case content: XmlSchemaSimpleContent =>
            // xs:simpleContent
            content.getContent match {
              case extension: XmlSchemaSimpleContentExtension =>
                val baseStructField = getBaseStructField(xmlSchema,
                  xmlSchema.getParent.getTypeByQName(extension.getBaseTypeName))
                val attributes = extension.getAttributes.asScala.map {
                  case attribute: XmlSchemaAttribute =>
                    val baseStructField = getBaseStructField(xmlSchema,
                      xmlSchema.getParent.getTypeByQName(attribute.getSchemaTypeName))
                    StructField(s"_${attribute.getName}", baseStructField.dataType,
                      attribute.getUse != XmlSchemaUse.REQUIRED)
                }
                schemaNameToStructFieldMap += (schemaType.getName ->
                  StructField(complexType.getName, StructType(baseStructField +: attributes)))
                StructField(complexType.getName, StructType(baseStructField +: attributes))
            }
          case content: XmlSchemaComplexContent =>
            content.getContent match {
              case extension: XmlSchemaSimpleContentExtension =>
                val baseStructField = getBaseStructField(xmlSchema,
                  xmlSchema.getParent.getTypeByQName(extension.getBaseTypeName))
                val attributes = extension.getAttributes.asScala.map {
                  case attribute: XmlSchemaAttribute =>
                    val baseStructField = getBaseStructField(xmlSchema,
                      xmlSchema.getParent.getTypeByQName(attribute.getSchemaTypeName))
                    StructField(s"_${attribute.getName}", baseStructField.dataType,
                      attribute.getUse != XmlSchemaUse.REQUIRED)
                }
                schemaNameToStructFieldMap += (schemaType.getName ->
                  StructField(complexType.getName, StructType(baseStructField +: attributes)))
                StructField(complexType.getName, StructType(baseStructField +: attributes))
              
              case extension: XmlSchemaComplexContentExtension =>
                val baseStructField = getBaseStructField(xmlSchema,
                  xmlSchema.getParent.getTypeByQName(extension.getBaseTypeName))
                val childFields =
                  extension.getParticle match {
                    // xs:all
                    case all: XmlSchemaAll =>
                      all.getItems.asScala.map {
                        case element: XmlSchemaElement =>
                          val baseStructField = getBaseStructField(xmlSchema,
                            element.getSchemaType)
                          val nullable = element.getMinOccurs == 0
                          if (element.getMaxOccurs == 1) {
                            StructField(element.getName, baseStructField.dataType, nullable)
                          } else {
                            StructField(element.getName,
                              ArrayType(baseStructField.dataType), nullable)
                          }
                      }
                    // xs:choice
                    case choice: XmlSchemaChoice =>
                      choice.getItems.asScala.map { case element: XmlSchemaElement =>
                        val baseStructField = getBaseStructField(xmlSchema, element.getSchemaType)
                        val nullable = element.getMinOccurs == 0
                        if (element.getMaxOccurs == 1) {
                          StructField(element.getName, baseStructField.dataType, nullable)
                        } else {
                          StructField(element.getName,
                            ArrayType(baseStructField.dataType), nullable)
                        }
                      }
                    // xs:sequence
                    case sequence: XmlSchemaSequence =>
                      // flatten xs:choice nodes
                      sequence.getItems.asScala.flatMap { member: XmlSchemaSequenceMember =>
                        member match {
                          case choice: XmlSchemaChoice =>
                            choice.getItems.asScala.map(e =>
                              (e.asInstanceOf[XmlSchemaElement], true))
                          case element: XmlSchemaElement =>
                            Seq((element, element.getMinOccurs == 0))
                        }
                      }.map { case (element: XmlSchemaElement, nullable) =>
                        val baseStructField = getBaseStructField(xmlSchema, element.getSchemaType)
                        if (element.getMaxOccurs == 1) {
                          StructField(element.getName, baseStructField.dataType, nullable)
                        } else {
                          StructField(element.getName,
                            ArrayType(baseStructField.dataType), nullable)
                        }
                      }
                  }
                val attributes = extension.getAttributes.asScala.map {
                  case attribute: XmlSchemaAttribute =>
                    val baseStructField = getBaseStructField(xmlSchema,
                      xmlSchema.getParent.getTypeByQName(attribute.getSchemaTypeName))
                    StructField(s"_${attribute.getName}", baseStructField.dataType,
                      attribute.getUse != XmlSchemaUse.REQUIRED)
                }
                schemaNameToStructFieldMap += (schemaType.getName ->
                  StructField(schemaType.getName,
                    StructType(baseStructField +: (childFields ++ attributes))))
                StructField(schemaType.getName,
                  StructType(baseStructField +: (childFields ++ attributes)))
            }
          case null => {
            val childFields =
              complexType.getParticle match {
                // xs:all
                case all: XmlSchemaAll =>
                  all.getItems.asScala.map {
                    case element: XmlSchemaElement =>
                      val baseStructField = getBaseStructField(xmlSchema, element.getSchemaType)
                      val nullable = element.getMinOccurs == 0
                      if (element.getMaxOccurs == 1) {
                        StructField(element.getName, baseStructField.dataType, nullable)
                      } else {
                        StructField(element.getName,
                          ArrayType(baseStructField.dataType), nullable)
                      }
                  }
                // xs:choice
                case choice: XmlSchemaChoice =>
                  choice.getItems.asScala.map { case element: XmlSchemaElement =>
                    val baseStructField = getBaseStructField(xmlSchema, element.getSchemaType)
                    val nullable = element.getMinOccurs == 0
                    if (element.getMaxOccurs == 1) {
                      StructField(element.getName, baseStructField.dataType, nullable)
                    } else {
                      StructField(element.getName, ArrayType(baseStructField.dataType), nullable)
                    }
                  }
                // xs:sequence
                case sequence: XmlSchemaSequence =>
                  // flatten xs:choice nodes
                  sequence.getItems.asScala.flatMap { member: XmlSchemaSequenceMember =>
                    member match {
                      case choice: XmlSchemaChoice =>
                        choice.getItems.asScala.map(e => (e.asInstanceOf[XmlSchemaElement], true))
                      case element: XmlSchemaElement => Seq((element, element.getMinOccurs == 0))
                    }
                  }.map { case (element: XmlSchemaElement, nullable) =>
                    val baseStructField = getBaseStructField(xmlSchema, element.getSchemaType)
                    if (element.getMaxOccurs == 1) {

                      StructField(element.getName, baseStructField.dataType, nullable)
                    } else {
                      StructField(element.getName, ArrayType(baseStructField.dataType), nullable)
                    }
                  }
              }
            val attributes = complexType.getAttributes.asScala.map {
              case attribute: XmlSchemaAttribute =>
                val baseStructField = getBaseStructField(xmlSchema,
                  xmlSchema.getParent.getTypeByQName(attribute.getSchemaTypeName))
                StructField(s"_${attribute.getName}", baseStructField.dataType,
                  attribute.getUse != XmlSchemaUse.REQUIRED)
            }
            schemaNameToStructFieldMap += (schemaType.getName ->
              StructField(complexType.getName, StructType(childFields ++ attributes)))
            StructField(complexType.getName, StructType(childFields ++ attributes))
          }
        }
      case unsupported =>
        throw new IllegalArgumentException(s"Unsupported schema element type: $unsupported")
    }
  }

  private def getBaseStructField(xmlSchema: XmlSchema, schemaType: XmlSchemaType): StructField = {
    if (schemaNameToStructFieldMap.contains(schemaType.getName)){
      return schemaNameToStructFieldMap.getOrElse(schemaType.getName, null);
    } else {
      val schemaType1 = getStructField(xmlSchema, schemaType)
      return schemaType1
    }
  }
  private def getStructType(xmlSchema: XmlSchema): StructType = {
    val (qName, schemaElement) = xmlSchema.getElements.asScala.head
    val schemaType = schemaElement.getSchemaType
    if (schemaType.isAnonymous) {
      schemaType.setName(qName.getLocalPart)
    }
    val rootType = getStructField(xmlSchema, schemaType)
    StructType(Seq(StructField(rootType.name, rootType.dataType, schemaElement.getMinOccurs == 0)))
  }
}
