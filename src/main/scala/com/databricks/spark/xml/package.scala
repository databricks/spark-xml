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
package com.databricks.spark

import scala.collection.Map

import com.databricks.spark.xml.util.XmlFile
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.StructType


package object xml {


  private def withExpr(expr: Expression): Column = new Column(expr)

  /**
    * Parses a column containing a XML string into a `StructType` with the specified schema.
    *
    * @param e a string column containing XML data
    * @param schema the schema to use when parsing the XML string
    */
  @Experimental
  implicit def from_xml(e: Column, schema: StructType): Column = {
    from_xml(e, schema, Map.empty[String, String])
  }

  /**
    * Parses a column containing a XML string into a `StructType` with the specified schema.
    *
    * @param e a string column containing XML data
    * @param schema the schema to use when parsing the XML string
    * @param options key-value pairs that correspond to those supported by [[XmlOptions]]
    */
  @Experimental
  implicit def from_xml(e: Column, schema: StructType, options: Map[String, String]): Column =
    withExpr {

    val map: Map[String, String] = options + ("isFunction" -> "true")
    val expr: Expression = CatalystSqlParser.parseExpression(e.toString())
    XmlDataToCatalyst(expr, schema, XmlOptions(map.toMap))
  }

  /**
   * Adds a method, `xmlFile`, to [[SQLContext]] that allows reading XML data.
   */
  implicit class XmlContext(sqlContext: SQLContext) extends Serializable {
    @deprecated("Use read.format(\"xml\") or read.xml", "0.4.0")
    def xmlFile(
        filePath: String,
        rowTag: String = XmlOptions.DEFAULT_ROW_TAG,
        samplingRatio: Double = 1.0,
        excludeAttribute: Boolean = false,
        treatEmptyValuesAsNulls: Boolean = false,
        failFast: Boolean = false,
        attributePrefix: String = XmlOptions.DEFAULT_ATTRIBUTE_PREFIX,
        valueTag: String = XmlOptions.DEFAULT_VALUE_TAG,
        charset: String = XmlOptions.DEFAULT_CHARSET): DataFrame = {

      val parameters = Map(
        "rowTag" -> rowTag,
        "samplingRatio" -> samplingRatio.toString,
        "excludeAttribute" -> excludeAttribute.toString,
        "treatEmptyValuesAsNulls" -> treatEmptyValuesAsNulls.toString,
        "failFast" -> failFast.toString,
        "attributePrefix" -> attributePrefix,
        "valueTag" -> valueTag,
        "charset" -> charset)
      val xmlRelation = XmlRelation(
        () => XmlFile.withCharset(sqlContext.sparkContext, filePath, charset, rowTag),
        location = Some(filePath),
        parameters = parameters.toMap)(sqlContext)
      sqlContext.baseRelationToDataFrame(xmlRelation)
    }
  }

  /**
   * Adds a method, `saveAsXmlFile`, to [[DataFrame]] that allows writing XML data.
   * If compressionCodec is not null the resulting output will be compressed.
   * Note that a codec entry in the parameters map will be ignored.
   */
  implicit class XmlSchemaRDD(dataFrame: DataFrame) {
    @deprecated("Use write.format(\"xml\") or write.xml", "0.4.0")
    def saveAsXmlFile(
        path: String, parameters: Map[String, String] = Map(),
        compressionCodec: Class[_ <: CompressionCodec] = null): Unit = {
      val mutableParams = collection.mutable.Map(parameters.toSeq: _*)
      val safeCodec = mutableParams.get("codec")
        .orElse(Option(compressionCodec).map(_.getCanonicalName))
        .orNull
      mutableParams.put("codec", safeCodec)
      XmlFile.saveAsXmlFile(dataFrame, path, mutableParams.toMap)
    }
  }

  /**
   * Adds a method, `xml`, to DataFrameReader that allows you to read avro files using
   * the DataFileReader
   */
  implicit class XmlDataFrameReader(reader: DataFrameReader) {
    def xml: String => DataFrame = reader.format("com.databricks.spark.xml").load
  }

  /**
   * Adds a method, `xml`, to DataFrameWriter that allows you to write avro files using
   * the DataFileWriter
   */
  implicit class XmlDataFrameWriter[T](writer: DataFrameWriter[T]) {
    // Note that writing a XML file from [[DataFrame]] having a field [[ArrayType]] with
    // its element as [[ArrayType]] would have an additional nested field for the element.
    // For example, the [[DataFrame]] having a field below,
    //
    //   fieldA [[data1, data2]]
    //
    // would produce a XML file below.
    //
    //   <fieldA>
    //       <item>data1</item>
    //   </fieldA>
    //   <fieldA>
    //       <item>data2</item>
    //   </fieldA>
    //
    // Namely, roundtrip in writing and reading can end up in different schema structure.
    def xml: String => Unit = writer.format("com.databricks.spark.xml").save
  }
}
