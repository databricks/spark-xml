/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql

import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.sql.xml.util.XmlFile

package object xml {
  /**
   * Adds a method, `xmlFile`, to SQLContext that allows reading XML data.
   */
  implicit class XmlContext(sqlContext: SQLContext) extends Serializable {
    def xmlFile(
                 filePath: String,
                 mode: String = "PERMISSIVE",
                 rootTag: String,
                 samplingRatio: Double = 1.0,
                 includeAttributeFlag: Boolean = false,
                 treatEmptyValuesAsNulls: Boolean = false,
                 charset: String = XmlFile.DEFAULT_CHARSET.name()
                 ): DataFrame = {

      val xmlRelation = XmlRelation(
        () => XmlFile.withCharset(sqlContext.sparkContext, filePath, charset, rootTag),
        location = Some(filePath),
        parseMode = mode,
        rootTag = rootTag,
        samplingRatio = samplingRatio,
        includeAttributeFlag = includeAttributeFlag,
        treatEmptyValuesAsNulls = treatEmptyValuesAsNulls)(sqlContext)
      sqlContext.baseRelationToDataFrame(xmlRelation)
    }
  }

  implicit class XmlSchemaRDD(dataFrame: DataFrame) {

    // TODO: XML write also should be supported.
    def saveAsXmlFile(path: String, parameters: Map[String, String] = Map(),
                      compressionCodec: Class[_ <: CompressionCodec] = null): Unit = {
      throw new UnsupportedOperationException("Writing XML is currently not supported.")
    }
  }
}
