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
      nullable: Boolean = true,
      treatEmptyValuesAsNulls: Boolean = false): Any = {
    if (datum == "" && nullable && (!castType.isInstanceOf[StringType] || treatEmptyValuesAsNulls)){
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
        case _: BooleanType => datum.toBoolean
        case _: DecimalType => new BigDecimal(datum.replaceAll(",", ""))
        // TODO(hossein): would be good to support other common timestamp formats
        case _: TimestampType => Timestamp.valueOf(datum)
        // TODO(hossein): would be good to support other common date formats
        case _: DateType => Date.valueOf(datum)
        case _: StringType => datum
        case _ => throw new RuntimeException(s"Unsupported type: ${castType.typeName}")
      }
    }
  }

  /**
   * Helper method that converts string representation of a character to actual character.
   * It handles some Java escaped strings and throws exception if given string is longer than one
   * character.
   *
   */
  @throws[IllegalArgumentException]
  private[xml] def toChar(str: String): Char = {
    if (str.charAt(0) == '\\') {
      str.charAt(1)
      match {
        case 't' => '\t'
        case 'r' => '\r'
        case 'b' => '\b'
        case 'f' => '\f'
        case '\"' => '\"' // In case user changes quote char and uses \" as delimiter in options
        case '\'' => '\''
        case 'u' if str == """\u0000""" => '\u0000'
        case _ =>
          throw new IllegalArgumentException(s"Unsupported special character for delimiter: $str")
      }
    } else if (str.length == 1) {
      str.charAt(0)
    } else {
      throw new IllegalArgumentException(s"Delimiter cannot be more than one character: $str")
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

  private[xml] def signSafeToLong(value: String): Long = {
    if (value.startsWith("+")) {
      val data = value.substring(1)
      TypeCast.castTo(data, LongType).asInstanceOf[Long]
    } else if (value.startsWith("-")) {
      val data = value.substring(1)
      -TypeCast.castTo(data, LongType).asInstanceOf[Long]
    } else {
      val data = value
      TypeCast.castTo(data, LongType).asInstanceOf[Long]
    }
  }

  private[xml] def signSafeToDouble(value: String): Double = {
    if (value.startsWith("+")) {
      val data = value.substring(1)
      TypeCast.castTo(data, DoubleType).asInstanceOf[Double]
    } else if (value.startsWith("-")) {
      val data = value.substring(1)
     -TypeCast.castTo(data, DoubleType).asInstanceOf[Double]
    } else {
      val data = value
      TypeCast.castTo(data, DoubleType).asInstanceOf[Double]
    }
  }

  private[xml] def signSafeToInt(value: String): Int = {
    if (value.startsWith("+")) {
      val data = value.substring(1)
      TypeCast.castTo(data, IntegerType).asInstanceOf[Int]
    } else if (value.startsWith("-")) {
      val data = value.substring(1)
      -TypeCast.castTo(data, IntegerType).asInstanceOf[Int]
    } else {
      val data = value
      TypeCast.castTo(data, IntegerType).asInstanceOf[Int]
    }
  }

  private[xml] def signSafeToFloat(value: String): Float = {
    if (value.startsWith("+")) {
      val data = value.substring(1)
      TypeCast.castTo(data, FloatType).asInstanceOf[Float]
    } else if (value.startsWith("-")) {
      val data = value.substring(1)
      -TypeCast.castTo(data, FloatType).asInstanceOf[Float]
    } else {
      val data = value
      TypeCast.castTo(data, FloatType).asInstanceOf[Float]
    }
  }
}
