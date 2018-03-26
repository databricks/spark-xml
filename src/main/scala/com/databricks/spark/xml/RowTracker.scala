/*
 * © Copyright 2018 HP Development Company, L.P.
 * SPDX-License-Identifier: Apache-2.0
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

import scala.collection.mutable

/**
  * Options for the XML data source.
  */
private[xml] class RowTracker(xmlOptions: XmlOptions)
  extends Serializable {
  private val trackErrors = xmlOptions.columnNameOfCorruptFields != null && xmlOptions.trackRows
  private val trackExtras = xmlOptions.columnNameOfExtraFields != null && xmlOptions.trackRows
  private val pathSeperator = "/"
  private val pathStack: mutable.Stack[String] = mutable.Stack[String]()
  private val _extraFields: mutable.Stack[String] = mutable.Stack[String]()
  private val _errorFields: mutable.Stack[String] = mutable.Stack[String]()

  // TODO This might be really inefficient. Consider a different implementation.
  private def getPathString(attribute: String): String = {
    if (attribute != null) {
      pathStack.push(attribute)
      val path = pathStack.reverse.mkString(pathSeperator)
      pathStack.pop()
      path
    }
    else {
      pathStack.reverse.mkString(pathSeperator)
    }
  }

  def pushPath(path: String): Unit = {
    if (xmlOptions.trackRows) {
      pathStack.push(path)
    }
  }

  def popPath(): Unit = {
    if (xmlOptions.trackRows) {
      pathStack.pop()
    }
  }

  def pushExtra(attribute: String = null): Unit = {
    if (trackExtras) {
      _extraFields.push(getPathString(attribute))
    }
  }

  def pushError(attribute: String = null): Unit = {
    if (trackErrors) {
      _errorFields.push(getPathString(attribute))
    }
  }

  def extraFields: mutable.Stack[String] = _extraFields.distinct

  def errorFields: mutable.Stack[String] = _errorFields.distinct

}