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
  charset: String = XmlOptions.DEFAULT_CHARSET,
  codec: String = null,
  rowTag: String = XmlOptions.DEFAULT_ROW_TAG,
  rootTag: String = XmlOptions.DEFAULT_ROOT_TAG,
  samplingRatio: Double = 1.0,
  excludeAttributeFlag: Boolean = false,
  treatEmptyValuesAsNulls: Boolean = false,
  failFastFlag: Boolean = false,
  attributePrefix: String = XmlOptions.DEFAULT_ATTRIBUTE_PREFIX,
  valueTag: String = XmlOptions.DEFAULT_VALUE_TAG,
  nullValue: String = XmlOptions.DEFAULT_NULL_VALUE,
  headingContents: String = XmlOptions.DEFAULT_HEADING_CONTENTS
)

private[xml] object XmlOptions {
  val DEFAULT_ATTRIBUTE_PREFIX = "@"
  val DEFAULT_VALUE_TAG = "#VALUE"
  val DEFAULT_ROW_TAG = "ROW"
  val DEFAULT_ROOT_TAG = "ROWS"
  val DEFAULT_CHARSET = "UTF-8"
  val DEFAULT_NULL_VALUE = "null"
  val DEFAULT_HEADING_CONTENTS = "<?xml version=\"1.0\"?>"

  def createFromConfigMap(parameters: Map[String, String]): XmlOptions = XmlOptions(
    charset = parameters.getOrElse("charset", DEFAULT_CHARSET),
    codec = parameters.get("codec").orNull,
    rowTag = parameters.getOrElse("rowTag", DEFAULT_ROW_TAG),
    rootTag = parameters.getOrElse("rootTag", DEFAULT_ROOT_TAG),
    samplingRatio = parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0),
    excludeAttributeFlag = parameters.get("excludeAttribute").map(_.toBoolean).getOrElse(false),
    treatEmptyValuesAsNulls =
      parameters.get("treatEmptyValuesAsNulls").map(_.toBoolean).getOrElse(false),
    failFastFlag = parameters.get("failFast").map(_.toBoolean).getOrElse(false),
    attributePrefix = parameters.getOrElse("attributePrefix", DEFAULT_ATTRIBUTE_PREFIX),
    valueTag = parameters.getOrElse("valueTag", DEFAULT_VALUE_TAG),
    nullValue = parameters.getOrElse("nullValue", DEFAULT_NULL_VALUE),
    headingContents = parameters.getOrElse("headingContents", DEFAULT_HEADING_CONTENTS)
  )
}
