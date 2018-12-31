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
package com.databricks.spark.xml.util

import java.math.BigDecimal
import java.sql.{Date, Timestamp}
import java.text.NumberFormat
import java.util.Locale

import scala.util.Try
import scala.util.control.Exception._

import org.apache.spark.sql.types._
import com.databricks.spark.xml.XmlOptions

/**
 * Utility functions for type casting
 */
object TypeCast {

  /**
   * Casts given string datum to specified type.
   * Currently we do not support complex types (ArrayType, MapType, StructType).
   *
   * For string types, this is simply the datum. For other types.
   * For other nullable types, this is null if the string datum is empty.
   *
   * @param datum string value
   * @param castType SparkSQL type
   */
  private[xml] def castTo(
      datum: String,
      castType: DataType,
      options: XmlOptions,
      nullable: Boolean = true): Any = {
    if (datum == options.nullValue &&
      nullable ||
      (options.treatEmptyValuesAsNulls && datum == "")){
      null
    } else {
      castType match {
        case _: ByteType => datum.toByte
        case _: ShortType => datum.toShort
        case _: IntegerType => datum.toInt
        case _: LongType => datum.toLong
        case _: FloatType => Try(datum.toFloat)
          .getOrElse(NumberFormat.getInstance(Locale.getDefault).parse(datum).floatValue())
        case _: DoubleType => Try(datum.toDouble)
          .getOrElse(NumberFormat.getInstance(Locale.getDefault).parse(datum).doubleValue())
        case _: BooleanType => parseXmlBoolean(datum)
        case _: DecimalType => new BigDecimal(datum.replaceAll(",", ""))
        case _: TimestampType => Timestamp.valueOf(datum)
        case _: DateType => Date.valueOf(datum)
        case _: StringType => datum
        case _ => throw new IllegalArgumentException(s"Unsupported type: ${castType.typeName}")
      }
    }
  }

  private def parseXmlBoolean(s: String): Boolean = {
    s.toLowerCase match {
      case "true" => true
      case "false" => false
      case "1" => true
      case "0" => false
      case _ => throw new IllegalArgumentException(s"For input string: $s")
    }
  }

  // TODO: This function unnecessarily does type dispatch. Should merge it with `castTo`.
  private[xml] def convertTo(
      datum: String,
      dataType: DataType,
      options: XmlOptions): Any = {
    val value = if (datum != null && options.ignoreSurroundingSpaces) {
      datum.trim()
    } else {
      datum
    }
    dataType match {
      case NullType => castTo(value, StringType, options)
      case LongType => signSafeToLong(value, options)
      case DoubleType => signSafeToDouble(value, options)
      case BooleanType => castTo(value, BooleanType, options)
      case StringType => castTo(value, StringType, options)
      case DateType => castTo(value, DateType, options)
      case TimestampType => castTo(value, TimestampType, options)
      case FloatType => signSafeToFloat(value, options)
      case ByteType => castTo(value, ByteType, options)
      case ShortType => castTo(value, ShortType, options)
      case IntegerType => signSafeToInt(value, options)
      case dt: DecimalType => castTo(value, dt, options)
      case _ => throw new IllegalArgumentException(
        s"Failed to parse a value for data type $dataType.")
    }
  }

  /**
   * Helper method that checks and cast string representation of a numeric types.
   */
  private[xml] def isBoolean(value: String): Boolean = {
    value.toLowerCase match {
      case "true" | "false" => true
      case _ => false
    }
  }

  private[xml] def isDouble(value: String): Boolean = {
    val signSafeValue: String = if (value.startsWith("+") || value.startsWith("-")) {
      value.substring(1)
    } else {
      value
    }
    (allCatch opt signSafeValue.toDouble).isDefined
  }

  private[xml] def isInteger(value: String): Boolean = {
    val signSafeValue: String = if (value.startsWith("+") || value.startsWith("-")) {
      value.substring(1)
    } else {
      value
    }
    (allCatch opt signSafeValue.toInt).isDefined
  }

  private[xml] def isLong(value: String): Boolean = {
    val signSafeValue: String = if (value.startsWith("+") || value.startsWith("-")) {
      value.substring(1)
    } else {
      value
    }
    (allCatch opt signSafeValue.toLong).isDefined
  }

  private[xml] def isTimestamp(value: String): Boolean = {
    (allCatch opt Timestamp.valueOf(value)).isDefined
  }

  private[xml] def signSafeToLong(value: String, options: XmlOptions): Long = {
    if (value.startsWith("+")) {
      val data = value.substring(1)
      TypeCast.castTo(data, LongType, options).asInstanceOf[Long]
    } else if (value.startsWith("-")) {
      val data = value.substring(1)
      -TypeCast.castTo(data, LongType, options).asInstanceOf[Long]
    } else {
      val data = value
      TypeCast.castTo(data, LongType, options).asInstanceOf[Long]
    }
  }

  private[xml] def signSafeToDouble(value: String, options: XmlOptions): Double = {
    if (value.startsWith("+")) {
      val data = value.substring(1)
      TypeCast.castTo(data, DoubleType, options).asInstanceOf[Double]
    } else if (value.startsWith("-")) {
      val data = value.substring(1)
     -TypeCast.castTo(data, DoubleType, options).asInstanceOf[Double]
    } else {
      val data = value
      TypeCast.castTo(data, DoubleType, options).asInstanceOf[Double]
    }
  }

  private[xml] def signSafeToInt(value: String, options: XmlOptions): Int = {
    if (value.startsWith("+")) {
      val data = value.substring(1)
      TypeCast.castTo(data, IntegerType, options).asInstanceOf[Int]
    } else if (value.startsWith("-")) {
      val data = value.substring(1)
      -TypeCast.castTo(data, IntegerType, options).asInstanceOf[Int]
    } else {
      val data = value
      TypeCast.castTo(data, IntegerType, options).asInstanceOf[Int]
    }
  }

  private[xml] def signSafeToFloat(value: String, options: XmlOptions): Float = {
    if (value.startsWith("+")) {
      val data = value.substring(1)
      TypeCast.castTo(data, FloatType, options).asInstanceOf[Float]
    } else if (value.startsWith("-")) {
      val data = value.substring(1)
      -TypeCast.castTo(data, FloatType, options).asInstanceOf[Float]
    } else {
      val data = value
      TypeCast.castTo(data, FloatType, options).asInstanceOf[Float]
    }
  }
}
