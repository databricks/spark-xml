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
package com.databricks.spark.xml.parsers.stax

import scala.collection.Map

import com.sun.xml.internal.txw2.output.IndentingXMLStreamWriter

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

// This class is borrowed from Spark json datasource.
private[xml] object StaxXmlGenerator {
  /** Transforms a single Row to XML
    *
    * @param rowSchema the schema object used for conversion
    * @param row The row to convert
    * @param writer a XML writer object
    * @param nullValue replacement for null.
    */
  def apply(rowSchema: StructType,
            tag: String,
            writer: IndentingXMLStreamWriter,
            nullValue: String)(row: Row): Unit = {
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
        v.foreach { p =>
          writer.writeStartElement("item")
          writeElement(ty, p)
          writer.writeEndElement()
        }

      case (MapType(kv, vt, _), mv: Map[_, _]) =>
        mv.foreach {
          case (k, v) =>
            (vt, v) match {
              case (ArrayType(ty, _), v: Seq[_]) =>
                v.foreach { p =>
                  writer.writeStartElement(k.toString)
                  writeElement(ty, p)
                  writer.writeEndElement()
                }
              case _ =>
                writer.writeStartElement(k.toString)
                writeElement(vt, v)
                writer.writeEndElement()
            }
        }

      case (StructType(ty), v: Row) =>
        ty.zip(v.toSeq).foreach {
          case (field, v) =>
            (field.dataType, v) match {
              case (ArrayType(ty, _), v: Seq[_]) =>
                v.foreach { p =>
                  writer.writeStartElement(field.name)
                  writeElement(ty, p)
                  writer.writeEndElement()
                }
              case _ =>
                writer.writeStartElement(field.name)
                writeElement(field.dataType, v)
                writer.writeEndElement()
            }
        }

      case (dt, v) =>
        sys.error(
          s"Failed to convert value $v (class of ${v.getClass}}) with the type of $dt to XML.")
    }
    writer.writeStartElement(tag)
    writeElement(rowSchema, row)
    writer.writeEndElement()
  }
}
