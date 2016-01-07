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

// This class is borrowed from Spark json datasource.
private[xml] object StaxXmlGenerator {

  /** Transforms a single Row to XML
    *
    * @param schema the schema object used for conversion
    * @param row The row to convert
    * @param writer a XML writer object
    * @param nullValue replacement for null.
    */
  def apply(schema: StructType,
            tag: String,
            writer: IndentingXMLStreamWriter,
            nullValue: String,
            attributePrefix: String,
            valueTag: String)(row: Row): Unit = {
    def writeChild(name: String, vt: DataType, v: Any): Unit = {
      if (name.startsWith(attributePrefix)) {
        (vt, v) match {
          case (_, null) | (NullType, _) =>
            writer.writeAttribute(name.substring(1), nullValue)
          case _ =>
            writer.writeAttribute(name.substring(1), v.toString)
        }
      } else if (name == valueTag) {
        writeElement(vt, v)
      } else {
        (vt, v) match {
          case (ArrayType(ty, _), v: Seq[_]) =>
            v.foreach { e =>
              writer.writeStartElement(name)
              writeElement(ty, e)
              writer.writeEndElement()
            }
          case _ =>
            writer.writeStartElement(name)
            writeElement(vt, v)
            writer.writeEndElement()
        }
      }
    }

    def writeElement: (DataType, Any) => Unit = {
      case (_, null) | (NullType, _) => writer.writeCharacters(nullValue)
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
      case (DateType, v) => writer.writeCharacters(v.toString)
      case (udt: UserDefinedType[_], v) => writeElement(udt.sqlType, udt.serialize(v))

      // For the case roundtrip in reading and writing XML files, [[ArrayType]] cannot have
      // [[ArrayType]] as element type. It always wraps the element with [[StructType]]. So,
      // this case only can happen when we convert a normal [[DataFrame]] to XML file.
      // When [[ArrayType]] has [[ArrayType]] as elements, it is confusing what is element name
      // for XML file. Now, it is "item" but this might have to be according the parent field name.
      case (ArrayType(ty, _), v: Seq[_]) =>
        v.foreach { e =>
          writeChild("item", ty, e)
        }

      case (MapType(kv, vt, _), mv: Map[_, _]) =>
        mv.foreach {
          case (k, v) =>
            writeChild(k.toString, vt, v)
        }

      case (StructType(ty), r: Row) =>
        ty.zip(r.toSeq).foreach {
          case (field, v) =>
            writeChild(field.name, field.dataType, v)
        }

      case (dt, v) =>
        sys.error(
          s"Failed to convert value $v (class of ${v.getClass}}) in type $dt to XML.")
    }

    val (attributes, elements) = schema.zip(row.toSeq).partition {
      case (f, v) => f.name.startsWith(attributePrefix)
    }
    // Writing attributes
    writer.writeStartElement(tag)
    attributes.foreach {
      case (f, v) =>
        writer.writeAttribute(f.name.substring(1), v.toString)
    }
    // Writing elements
    val (names, values) = elements.unzip
    val elementSchema = StructType(schema.filter(names.contains))
    val elementRow = Row.fromSeq(row.toSeq.filter(values.contains))
    writeElement(elementSchema, elementRow)
    writer.writeEndElement()
  }
}
