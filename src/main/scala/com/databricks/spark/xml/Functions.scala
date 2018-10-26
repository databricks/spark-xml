package com.databricks.spark.xml

import scala.language.implicitConversions
import scala.reflect.runtime.universe.{TypeTag, typeTag}
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParserUtils}
import org.apache.spark.sql.execution.SparkSqlParser

import scala.collection.Map

object Functions {

  private def withExpr(expr: Expression): Column = new Column(expr)

  /**
    * Parses a column containing a XML string into a `StructType` with the specified schema.
    * Returns `null` in the case of an non-parseable string.
    *
    * @param e a string column containing XML data
    * @param schema the schema to use when parsing the XML strin

    */
  @Experimental
  def from_xml(e: Column, schema: StructType): Column = {
    val parameters = Map("isFunction" -> "true")
    from_xml(e, schema, parameters)
  }


  @Experimental
  def from_xml(e: Column, schema: StructType, options: Map[String, String]): Column = withExpr {
    val map: Map[String, String] = options + ("isFunction" -> "true")
    val expr: Expression = CatalystSqlParser.parseExpression(e.toString())
    XmlDataToCatalyst(expr, schema, XmlOptions(map.toMap))
  }
}
