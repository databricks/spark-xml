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

import java.nio.charset.StandardCharsets

import com.databricks.spark.xml.util._

/**
 * Options for the XML data source.
 */
private[xml] class XmlOptions(
    @transient private val parameters: Map[String, String])
  extends Serializable {

  def this() = this(Map.empty)

  val charset = parameters.getOrElse("charset", XmlOptions.DEFAULT_CHARSET)
  val codec = parameters.get("compression").orElse(parameters.get("codec")).orNull
  val rowTag = parameters.getOrElse("rowTag", XmlOptions.DEFAULT_ROW_TAG)
  require(rowTag.nonEmpty, "'rowTag' option should not be empty string.")
  val rootTag = parameters.getOrElse("rootTag", XmlOptions.DEFAULT_ROOT_TAG)
  val samplingRatio = parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)
  require(samplingRatio > 0, s"samplingRatio ($samplingRatio) should be greater than 0")
  val excludeAttributeFlag = parameters.get("excludeAttribute").map(_.toBoolean).getOrElse(false)
  val treatEmptyValuesAsNulls =
    parameters.get("treatEmptyValuesAsNulls").map(_.toBoolean).getOrElse(false)
  val attributePrefix =
    parameters.getOrElse("attributePrefix", XmlOptions.DEFAULT_ATTRIBUTE_PREFIX)
  require(attributePrefix.nonEmpty, "'attributePrefix' option should not be empty string.")
  val valueTag = parameters.getOrElse("valueTag", XmlOptions.DEFAULT_VALUE_TAG)
  require(valueTag.nonEmpty, "'valueTag' option should not be empty string.")
  require(valueTag != attributePrefix,
    "'valueTag' and 'attributePrefix' options should not be the same.")
  val nullValue = parameters.getOrElse("nullValue", XmlOptions.DEFAULT_NULL_VALUE)
  val columnNameOfCorruptRecord =
    parameters.getOrElse("columnNameOfCorruptRecord", "_corrupt_record")
  val ignoreSurroundingSpaces =
    parameters.get("ignoreSurroundingSpaces").map(_.toBoolean).getOrElse(false)
  val parseMode = ParseMode.fromString(parameters.getOrElse("mode", PermissiveMode.name))
  val inferSchema = parameters.get("inferSchema").map(_.toBoolean).getOrElse(true)
}

private[xml] object XmlOptions {
  val DEFAULT_ATTRIBUTE_PREFIX = "_"
  val DEFAULT_VALUE_TAG = "_VALUE"
  val DEFAULT_ROW_TAG = "ROW"
  val DEFAULT_ROOT_TAG = "ROWS"
  val DEFAULT_CHARSET: String = StandardCharsets.UTF_8.name
  val DEFAULT_NULL_VALUE: String = null

  def apply(parameters: Map[String, String]): XmlOptions = new XmlOptions(parameters)
}
