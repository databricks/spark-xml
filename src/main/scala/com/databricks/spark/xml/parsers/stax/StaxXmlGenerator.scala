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

import com.sun.xml.txw2.output.IndentingXMLStreamWriter

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

// This class is borrowed from Spark json datasource.
private[xml] object StaxXmlGenerator {
  /** Transforms a single Row to XML
    *
    * @param rowSchema the schema object used for conversion
    * @param writer a XML writer object
    * @param row The row to convert
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
      case (BinaryType, v: Array[Byte]) => writer.writeCharacters(v.toString)
      case (BooleanType, v: Boolean) => writer.writeCharacters(v.toString)
      case (DateType, v) => writer.writeCharacters(v.toString)
      case (udt: UserDefinedType[_], v) => writeElement(udt.sqlType, udt.serialize(v))

      case (MapType(kv, vv, _), v: Map[_, _]) =>
        v.foreach { p =>
          writer.writeStartElement(p._1.toString)
          writeElement(vv, p._2)
          writer.writeEndElement()
        }

      case (StructType(ty), v: Row) =>
        ty.zip(v.toSeq).foreach {
          case (_, null) =>
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
    }
    writer.writeStartElement(tag)
    writeElement(rowSchema, row)
    writer.writeEndElement()
  }
}
