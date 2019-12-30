/*
 * Copyright 2019 Databricks
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

package com.databricks.spark.xml

import javax.xml.stream.XMLInputFactory

import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.databricks.spark.xml.parsers.{StaxXmlParser, StaxXmlParserUtils}

case class XmlDataToCatalyst(
    child: Expression,
    schema: StructType,
    options: XmlOptions)
  extends UnaryExpression with CodegenFallback with ExpectsInputTypes {

  @transient
  private lazy val factory: XMLInputFactory = StaxXmlParserUtils.buildFactory()

  override lazy val dataType: DataType = schema

  override def checkInputDataTypes(): TypeCheckResult = schema match {
    case _: StructType =>
      super.checkInputDataTypes()
    case _ => TypeCheckResult.TypeCheckFailure(
      s"Input schema ${schema.simpleString} must be a struct or an array of structs.")
  }

  override def nullSafeEval(xml: Any): Any = xml match {
    case string: UTF8String =>
      CatalystTypeConverters.convertToCatalyst(
        StaxXmlParser.parseColumn(string.toString, schema, factory, options))
    case string: String =>
      StaxXmlParser.parseColumn(string.toString, schema, factory, options)
    case _ => null
  }

  override def inputTypes: Seq[DataType] = StringType :: Nil
}
