/*
 * Copyright 2019 Databricks
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

package com.databricks.spark.xml.util

import java.nio.file.Paths
import javax.xml.validation.{Schema, SchemaFactory}
import javax.xml.XMLConstants

import com.google.common.cache.{CacheBuilder, CacheLoader}

import org.apache.spark.SparkFiles

/**
 * Utilities for working with XSD validation.
 */
private[xml] object ValidatorUtil {

  // Parsing XSDs may be slow, so cache them by path:

  private val cache = CacheBuilder.newBuilder().softValues().build(
    new CacheLoader[String, Schema] {
      override def load(key: String): Schema = {
        // Handle case where file exists as specified
        var path = Paths.get(key)
        if (!path.toFile.exists()) {
          // Handle case where it was added with sc.addFile
          path = Paths.get(SparkFiles.get(key))
        }
        val schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
        schemaFactory.newSchema(path.toFile)
      }
    })

  /**
   * Parses the XSD at the given local path and caches it.
   *
   * @param path path to XSD
   * @return Schema for the file at that path
   */
  def getSchema(path: String): Schema = cache.get(path)
}
