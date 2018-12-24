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
package com.databricks.spark.xml.parsers

import scala.collection.Map

import com.sun.xml.internal.txw2.output.IndentingXMLStreamWriter

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import com.databricks.spark.xml.XmlOptions

// This class is borrowed from Spark json datasource.
private[xml] object StaxXmlGenerator {

  /** Transforms a single Row to XML
    *
    * @param schema the schema object used for conversion
    * @param writer a XML writer object
    * @param options options for XML datasource.
    * @param row The row to convert
    */
  def apply(
      schema: StructType,
      writer: IndentingXMLStreamWriter,
      options: XmlOptions)(row: Row): Unit = {
    def writeChildElement(name: String, dt: DataType, v: Any): Unit = (name, dt, v) match {
      // If this is meant to be value but in no child, write only a value
      case (_, _, null) | (_, NullType, _) if options.nullValue == null =>
        // Because usually elements having `null` do not exist, just do not write
        // elements when given values are `null`.
      case (_, _, _) if name == options.valueTag =>
        // If this is meant to be value but in no child, write only a value
        writeElement(dt, v)
      case (_, _, _) =>
        writer.writeStartElement(name)
        writeElement(dt, v)
        writer.writeEndElement()
    }

    def writeChild(name: String, dt: DataType, v: Any): Unit = {
      (dt, v) match {
        // If this is meant to be attribute, write an attribute
        case (_, null) | (NullType, _)
          if name.startsWith(options.attributePrefix) && name != options.valueTag =>
          Option(options.nullValue).foreach {
            writer.writeAttribute(name.substring(options.attributePrefix.length), _)
          }
        case _ if name.startsWith(options.attributePrefix) && name != options.valueTag =>
          writer.writeAttribute(name.substring(options.attributePrefix.length), v.toString)

        // For ArrayType, we just need to write each as XML element.
        case (ArrayType(ty, _), v: Seq[_]) =>
          v.foreach { e =>
            writeChildElement(name, ty, e)
          }
        // For other datatypes, we just write normal elements.
        case _ =>
          writeChildElement(name, dt, v)
      }
    }

    def writeElement(dt: DataType, v: Any): Unit = (dt, v) match {
      case (_, null) | (NullType, _) => writer.writeCharacters(options.nullValue)
      case (StringType, v: String) => writer.writeCharacters(v.toString)
      case (TimestampType, v: java.sql.Timestamp) => writer.writeCharacters(v.toString)
      case (IntegerType, v: Int) => writer.writeCharacters(v.toString)
      case (ShortType, v: Short) => writer.writeCharacters(v.toString)
      case (FloatType, v: Float) => writer.writeCharacters(v.toString)
      case (DoubleType, v: Double) => writer.writeCharacters(v.toString)
      case (LongType, v: Long) => writer.writeCharacters(v.toString)
      case (DecimalType(), v: java.math.BigDecimal) => writer.writeCharacters(v.toString)
      case (ByteType, v: Byte) => writer.writeCharacters(v.toString)
      case (BooleanType, v: Boolean) => writer.writeCharacters(v.toString)
      case (DateType, _) => writer.writeCharacters(v.toString)

      // For the case roundtrip in reading and writing XML files, [[ArrayType]] cannot have
      // [[ArrayType]] as element type. It always wraps the element with [[StructType]]. So,
      // this case only can happen when we convert a normal [[DataFrame]] to XML file.
      // When [[ArrayType]] has [[ArrayType]] as elements, it is confusing what is element name
      // for XML file. Now, it is "item" but this might have to be according the parent field name.
      case (ArrayType(ty, _), v: Seq[_]) =>
        v.foreach { e =>
          writeChild("item", ty, e)
        }

      case (MapType(_, vt, _), mv: Map[_, _]) =>
        val (attributes, elements) = mv.toSeq.partition { case (f, _) =>
          f.toString.startsWith(options.attributePrefix) && f.toString != options.valueTag
        }
        // We need to write attributes first before the value.
        (attributes ++ elements).foreach {
          case (k, v) =>
            writeChild(k.toString, vt, v)
        }

      case (StructType(ty), r: Row) =>
        val (attributes, elements) = ty.zip(r.toSeq).partition { case (f, _) =>
          f.name.startsWith(options.attributePrefix) && f.name != options.valueTag
        }
        // We need to write attributes first before the value.
        (attributes ++ elements).foreach {
          case (field, value) =>
            writeChild(field.name, field.dataType, value)
        }

      case (_, _) =>
        sys.error(
          s"Failed to convert value $v (class of ${v.getClass}) in type $dt to XML.")
    }

    val (attributes, elements) = schema.zip(row.toSeq).partition { case (f, _) =>
      f.name.startsWith(options.attributePrefix) && f.name != options.valueTag
    }
    // Writing attributes
    writer.writeStartElement(options.rowTag)
    attributes.foreach {
      case (f, v) if v == null || f.dataType == NullType =>
        Option(options.nullValue).foreach {
          writer.writeAttribute(f.name.substring(options.attributePrefix.length), _)
        }
      case (f, v) =>
        writer.writeAttribute(f.name.substring(options.attributePrefix.length), v.toString)
    }
    // Writing elements
    val (names, values) = elements.unzip
    val elementSchema = StructType(schema.filter(names.contains))
    val elementRow = Row.fromSeq(row.toSeq.filter(values.contains))
    writeElement(elementSchema, elementRow)
    writer.writeEndElement()
  }
}
