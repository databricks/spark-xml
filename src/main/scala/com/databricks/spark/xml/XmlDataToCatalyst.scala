package com.databricks.spark.xml

import com.databricks.spark.xml.parsers.{StaxXmlGenerator, StaxXmlParser}
import javax.xml.stream.events.XMLEvent
import javax.xml.stream.{EventFilter, XMLInputFactory, XMLStreamConstants}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ImplicitCastInputTypes, SpecializedGetters, UnaryExpression}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class XmlDataToCatalyst(child: Expression,
                             schema: DataType,
                             options: XmlOptions)
  extends UnaryExpression with CodegenFallback with ExpectsInputTypes {

  override lazy val dataType: DataType = schema

  override def checkInputDataTypes(): TypeCheckResult = schema match {
    case _: StructType | ArrayType(_: StructType, _) =>
      super.checkInputDataTypes()
    case _ => TypeCheckResult.TypeCheckFailure(
      s"Input schema ${schema.simpleString} must be a struct or an array of structs.")
  }

  @transient
  lazy val rowSchema: StructType = schema match {
    case st: StructType => st
    case ArrayType(st: StructType, _) => st
  }

  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      null
    } else {
      nullSafeEval(value)
    }
  }

  override def nullSafeEval(xml: Any): Any = {
    xml match {
      case string: UTF8String =>
        CatalystTypeConverters.convertToCatalyst(
          StaxXmlParser.parseColumn(string.getBytes, rowSchema, options))
      case string: String =>
        StaxXmlParser.parseColumn(string.getBytes, rowSchema, options)
      case _ => null
    }

  }

  override def inputTypes: Seq[DataType] = StringType :: Nil
}
