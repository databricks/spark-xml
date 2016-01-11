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
package com.databricks.spark.xml.parsers

import com.databricks.spark.xml.util.XmlFile

/**
 * Options for the XML data source.
 */
private[xml] case class XmlOptions(
  samplingRatio: Double = 1.0,
  excludeAttributeFlag: Boolean = false,
  treatEmptyValuesAsNulls: Boolean = false,
  failFastFlag: Boolean = false,
  attributePrefix: String = XmlFile.DEFAULT_ATTRIBUTE_PREFIX,
  valueTag: String = XmlFile.DEFAULT_VALUE_TAG
)

object XmlOptions {
  def createFromConfigMap(parameters: Map[String, String]): XmlOptions = XmlOptions(
    samplingRatio =
      parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0),
    excludeAttributeFlag =
      parameters.get("excludeAttribute").map(_.toBoolean).getOrElse(false),
    treatEmptyValuesAsNulls =
      parameters.get("treatEmptyValuesAsNulls").map(_.toBoolean).getOrElse(false),
    failFastFlag =
      parameters.get("failFast").map(_.toBoolean).getOrElse(false),
    attributePrefix =
      parameters.getOrElse("attributePrefix", XmlFile.DEFAULT_ATTRIBUTE_PREFIX),
    valueTag =
      parameters.getOrElse("valueTag", XmlFile.DEFAULT_VALUE_TAG)
  )
}