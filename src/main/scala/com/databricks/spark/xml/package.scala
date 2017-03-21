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

import org.apache.hadoop.io.compress.CompressionCodec

import org.apache.spark.sql.{DataFrame, SQLContext}
import com.databricks.spark.xml.util.{XmlFile, XmlRDD}

package object xml {
  /**
   * Adds a method, `xmlFile`, to [[SQLContext]] that allows reading XML data.
   */
  implicit class XmlContext(sqlContext: SQLContext) extends Serializable {
    @deprecated("Use DataFrameReader.read()", "0.4.0")
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
    @deprecated("Use DataFrameWriter.write()", "0.4.0")
    def saveAsXmlFile(
        path: String, parameters: Map[String, String] = Map(),
        compressionCodec: Class[_ <: CompressionCodec] = null): Unit = {
      val codecOpt = parameters.get("codec")
        .orElse(Option(compressionCodec).map(_.getCanonicalName))
      val paramsWithCodec = codecOpt.map(c => parameters + ("codec" -> c)).getOrElse(parameters)
      XmlFile.saveAsXmlFile(dataFrame, path, paramsWithCodec)
    }
  }
}
