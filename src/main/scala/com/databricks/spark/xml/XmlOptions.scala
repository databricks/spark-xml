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
package com.databricks.spark.xml

/**
 * Options for the XML data source.
 */
private[xml] case class XmlOptions(
  charset: String,
  rowTag: String,
  rootTag: String,
  samplingRatio: Double,
  excludeAttributeFlag: Boolean,
  treatEmptyValuesAsNulls: Boolean,
  failFastFlag: Boolean,
  attributePrefix: String,
  valueTag: String,
  nullValue: String
)

private[xml] object XmlOptions {
  val DEFAULT_ATTRIBUTE_PREFIX = "@"
  val DEFAULT_VALUE_TAG = "#VALUE"
  val DEFAULT_ROW_TAG = "ROW"
  val DEFAULT_ROOT_TAG = "ROWS"
  val DEFAULT_CHARSET = "UTF-8"
  val DEFAULT_NULL_VALUE = "null"

  def createFromConfigMap(confMap: Map[String, Any]): XmlOptions = {
    val parameters = confMap.map(pair => pair._1 -> pair._2.toString)
    XmlOptions(
      // TODO Support different encoding types.
      charset = parameters.getOrElse("charset", DEFAULT_CHARSET),
      rowTag = parameters.getOrElse("rowTag", DEFAULT_ROW_TAG),
      rootTag = parameters.getOrElse("rootTag", DEFAULT_ROOT_TAG),
      samplingRatio = parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0),
      excludeAttributeFlag = parameters.get("excludeAttribute").map(_.toBoolean).getOrElse(false),
      treatEmptyValuesAsNulls =
        parameters.get("treatEmptyValuesAsNulls").map(_.toBoolean).getOrElse(false),
      failFastFlag = parameters.get("failFast").map(_.toBoolean).getOrElse(false),
      attributePrefix = parameters.getOrElse("attributePrefix", DEFAULT_ATTRIBUTE_PREFIX),
      valueTag = parameters.getOrElse("valueTag", DEFAULT_VALUE_TAG),
      nullValue = parameters.getOrElse("nullValue", DEFAULT_NULL_VALUE)
    )
  }
}
