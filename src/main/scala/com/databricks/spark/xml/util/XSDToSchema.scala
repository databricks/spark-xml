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
        StructField("_VALUE", schemaType1)
      // xs:complexType
      case complexType: XmlSchemaComplexType =>
        complexType.getContentModel match {
          case content: XmlSchemaSimpleContent =>
            // xs:simpleContent
            content.getContent match {
              case extension: XmlSchemaSimpleContentExtension =>
                val baseStructField = getBaseStructField(xmlSchema,
                  xmlSchema.getParent.getTypeByQName(extension.getBaseTypeName))
                var attributes = ArrayBuffer.empty[StructField]
                extension.getAttributes.asScala.map {
                  case attribute: XmlSchemaAttribute =>
                    attributes += getAttributes(xmlSchema, attribute)
                  case attributeGroup: XmlSchemaAttributeGroup =>
                    attributes = attributes ++ getAttributesFromGroup(xmlSchema, attributeGroup)
                  case attributeGroupRef: XmlSchemaAttributeGroupRef =>
                    attributes = attributes ++
                      getAttributesFromGroupRef(xmlSchema, attributeGroupRef)
                }
                //                complexType.getN
                val metadata = getMetadata(complexType)
                val structField = if (baseStructField.dataType.isInstanceOf[StructType]) {
                  StructField(complexType.getName,
                    StructType(baseStructField.dataType.asInstanceOf[StructType].fields ++
                      attributes), metadata = metadata)
                } else {
                  StructField(complexType.getName, StructType(baseStructField +: attributes),
                    metadata = metadata)
                }
                schemaNameToStructFieldMap += (schemaType.getName -> structField)
                structField
            }
          case content: XmlSchemaComplexContent =>
            content.getContent match {
              case extension: XmlSchemaSimpleContentExtension =>
                val baseStructField = getBaseStructField(xmlSchema,
                  xmlSchema.getParent.getTypeByQName(extension.getBaseTypeName))
                var attributes = ArrayBuffer.empty[StructField]
                extension.getAttributes.asScala.map {
                  case attribute: XmlSchemaAttribute =>
                    attributes += getAttributes(xmlSchema, attribute)
                  case attributeGroup: XmlSchemaAttributeGroup =>
                    attributes = attributes ++ getAttributesFromGroup(xmlSchema, attributeGroup)
                  case attributeGroupRef: XmlSchemaAttributeGroupRef =>
                    attributes = attributes ++
                      getAttributesFromGroupRef(xmlSchema, attributeGroupRef)
                }
                val metadata = getMetadata(complexType)
                val structField = if (baseStructField.dataType.isInstanceOf[StructType]) {
                  StructField(complexType.getName,
                    StructType(baseStructField.dataType.asInstanceOf[StructType].fields
                      ++ attributes), metadata = metadata)
                } else {
                  StructField(complexType.getName, StructType(baseStructField +: attributes),
                    metadata = metadata)
                }
                schemaNameToStructFieldMap += (schemaType.getName -> structField)
                structField

              case extension: XmlSchemaComplexContentExtension =>
                val baseStructField = getBaseStructField(xmlSchema,
                  xmlSchema.getParent.getTypeByQName(extension.getBaseTypeName))
                val childFields = getChildFields(xmlSchema, extension.getParticle)
                var attributes = ArrayBuffer.empty[StructField]
                extension.getAttributes.asScala.map {
                  case attribute: XmlSchemaAttribute =>
                    attributes += getAttributes(xmlSchema, attribute)
                  case attributeGroup: XmlSchemaAttributeGroup =>
                    attributes = attributes ++ getAttributesFromGroup(xmlSchema, attributeGroup)
                  case attributeGroupRef: XmlSchemaAttributeGroupRef =>
                    attributes = attributes ++
                      getAttributesFromGroupRef(xmlSchema, attributeGroupRef)
                }
                val metadata = getMetadata(schemaType)
                val structField = if (baseStructField.dataType.isInstanceOf[StructType]) {
                  StructField(schemaType.getName,
                    StructType(baseStructField.dataType.asInstanceOf[StructType].fields ++
                      (childFields ++ attributes)), metadata = metadata)
                } else {
                  StructField(schemaType.getName,
                    StructType(baseStructField +: (childFields ++ attributes)), metadata = metadata)
                }
                schemaNameToStructFieldMap += (schemaType.getName -> structField)
                structField
            }
          case null => {
            val childFields = getChildFields(xmlSchema, complexType.getParticle)
            var attributes = ArrayBuffer.empty[StructField]
            complexType.getAttributes.asScala.map {
              case attribute: XmlSchemaAttribute =>
                attributes += getAttributes(xmlSchema, attribute)
              case attributeGroup: XmlSchemaAttributeGroup =>
                attributes = attributes ++ getAttributesFromGroup(xmlSchema, attributeGroup)
              case attributeGroupRef: XmlSchemaAttributeGroupRef =>
                attributes = attributes ++
                  getAttributesFromGroupRef(xmlSchema, attributeGroupRef)
            }
            val metadata = getMetadata(complexType)
            val structField = StructField(complexType.getName,
              StructType(childFields ++ attributes), metadata = metadata)
            schemaNameToStructFieldMap += (schemaType.getName -> structField)
            structField
          }
        }
      case null =>
        StructField("", StringType)
      case unsupported =>
        throw new IllegalArgumentException(s"Unsupported schema element type: $unsupported")
    }
  }

  private def getBaseStructField(xmlSchema: XmlSchema, schemaType: XmlSchemaType): StructField = {
    var name = ""
    if (null != schemaType && null != schemaType.getName) {
      name = schemaType.getName
    }
    schemaNameToStructFieldMap.getOrElse(name,
      getStructField(xmlSchema, schemaType))
  }

  private def getChildFields(xmlSchema: XmlSchema,
                             varType: XmlSchemaParticle): mutable.Buffer[StructField] = {
    varType match {
      // xs:all
      case all: XmlSchemaAll =>
        all.getItems.asScala.map {
          case element: XmlSchemaElement =>
            val elementStructField = getBaseStructField(xmlSchema,
              element.getSchemaType)
            val metadata = getMetadata(element)
            val nullable = element.getMinOccurs == 0
            if (element.getMaxOccurs == 1) {
              StructField(element.getName, elementStructField.dataType, nullable, metadata)
            } else {
              StructField(element.getName, ArrayType(elementStructField.dataType),
                nullable, metadata)
            }
        }
      // xs:choice
      case choice: XmlSchemaChoice =>
        var choiceFields = ArrayBuffer.empty[StructField]
        choice.getItems.asScala.map { member: XmlSchemaChoiceMember =>
          val childFields = getChildFields(xmlSchema, member.asInstanceOf[XmlSchemaParticle])
          if (choice.getMaxOccurs > 1) {
            childFields.iterator.foreach(e =>
              choiceFields += StructField(e.name, ArrayType(e.dataType),
                choice.getMinOccurs == 0, e.metadata))
          } else {
            choiceFields = choiceFields ++ childFields
          }
        }
        choiceFields

      case sequence: XmlSchemaSequence =>
        var seqFields = ArrayBuffer.empty[StructField]
        sequence.getItems.asScala.map { member: XmlSchemaSequenceMember =>
          val childFields = getChildFields(xmlSchema, member.asInstanceOf[XmlSchemaParticle])
          if (sequence.getMaxOccurs > 1) {
            childFields.iterator.foreach(e =>
              seqFields += StructField(e.name, ArrayType(e.dataType),
                sequence.getMinOccurs == 0, e.metadata))
            seqFields ++ childFields
          } else {
            seqFields = seqFields ++ childFields
          }
        }
        seqFields

      case groupRef: XmlSchemaGroupRef =>
        var groupFields = ArrayBuffer.empty[StructField]
        val group = xmlSchema.getGroupByName(groupRef.getRefName)
        val childFields = getChildFields(xmlSchema, group.getParticle)
        if (groupRef.getMaxOccurs > 1) {
          childFields.iterator.foreach( e =>
            groupFields += StructField(e.name, ArrayType(e.dataType),
              groupRef.getMinOccurs == 0, e.metadata))
          groupFields ++ childFields
        } else {
          groupFields = groupFields ++ childFields
        }
        groupFields

      case element: XmlSchemaElement =>
        var metadata = getMetadata(element)
        val nullable = element.getMinOccurs == 0
        if (null!=element.getRef && null!=element.getRef.getTargetQName) {
          val element1 = xmlSchema.getElementByName(element.getRef.getTargetQName)
          metadata = getMetadata(element1)
          val baseStructField = getBaseStructField(xmlSchema, element1.getSchemaType)
          if (element.getMaxOccurs > 1) {
            ArrayBuffer(StructField(element1.getName,
              ArrayType(baseStructField.dataType), nullable, metadata))
          } else {
            ArrayBuffer(
              StructField(element1.getName, baseStructField.dataType, nullable, metadata))
          }
        } else {
          val baseStructField = getBaseStructField(xmlSchema, element.getSchemaType)
          if (element.getMaxOccurs > 1) {
            ArrayBuffer(StructField(element.getName,
              ArrayType(baseStructField.dataType), nullable, metadata))
          } else {
            ArrayBuffer(StructField(element.getName, baseStructField.dataType, nullable, metadata))
          }
        }
      case null =>
        ArrayBuffer.empty[StructField]
    }
  }

  private def getMetadata(xmlSchemaNamed: XmlSchemaNamed): Metadata = {
    val baseUri = if (null != xmlSchemaNamed && null != xmlSchemaNamed.getQName &&
      null != xmlSchemaNamed.getQName.getNamespaceURI) {
      xmlSchemaNamed.getQName.getNamespaceURI
    } else {
      ""
    }
    new MetadataBuilder()
      .putString("baseUri", baseUri)
      .build()
  }

  private def getAttributes(xmlSchema: XmlSchema,
                            attribute: XmlSchemaAttribute):
  StructField = {
    val metadata = getMetadata(attribute)
    val attrBaseStructField = if (null != attribute.getSchemaTypeName) {
      getBaseStructField(xmlSchema,
        xmlSchema.getParent.getTypeByQName(attribute.getSchemaTypeName))
    } else {
      getBaseStructField(xmlSchema,
        attribute.getSchemaType)
    }
    StructField(s"_${attribute.getName}", attrBaseStructField.dataType,
      attribute.getUse != XmlSchemaUse.REQUIRED, metadata = metadata)
  }

  private def getAttributesFromGroup(xmlSchema: XmlSchema, attributeGroup: XmlSchemaAttributeGroup):
  mutable.Buffer[StructField] = {
    var attributeFields = ArrayBuffer.empty[StructField]
    attributeGroup.getAttributes.asScala.map {
      case attributeGroup: XmlSchemaAttributeGroup =>
        val fields = getAttributesFromGroup(xmlSchema, attributeGroup)
        attributeFields = attributeFields ++ fields
      case attribute: XmlSchemaAttribute =>
        attributeFields += getAttributes(xmlSchema, attribute)
      case ref: XmlSchemaAttributeGroupRef =>
        val group = xmlSchema.getParent.getAttributeGroupByQName(ref.getTargetQName)
        attributeFields = attributeFields ++ getAttributesFromGroup(xmlSchema, group)
    }
    attributeFields
  }

  private def getAttributesFromGroupRef(xmlSchema: XmlSchema,
                                        attributeGroupRef: XmlSchemaAttributeGroupRef):
  mutable.Buffer[StructField] = {
    val attributeGroup =
      xmlSchema.getParent.getAttributeGroupByQName(attributeGroupRef.getTargetQName)
    getAttributesFromGroup(xmlSchema, attributeGroup)
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
