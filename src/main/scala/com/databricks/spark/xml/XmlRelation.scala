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

import java.io.IOException

import org.apache.hadoop.fs.Path

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{PrunedScan, InsertableRelation, BaseRelation}
import org.apache.spark.sql.types._
import com.databricks.spark.xml.util.{InferSchema, XmlFile}
import com.databricks.spark.xml.parsers.StaxXmlParser

case class XmlRelation protected[spark] (
    baseRDD: () => RDD[String],
    locations: Seq[String],
    parameters: Map[String, String],
    userSchema: StructType = null)(@transient val sqlContext: SQLContext)
  extends BaseRelation
  with InsertableRelation
  with PrunedScan {

  // Hacky: ensure RDD's underlying data actually already exists early on
  baseRDD().partitions

  private val options = XmlOptions(parameters)

  override val schema: StructType = {
    Option(userSchema).getOrElse {
      InferSchema.infer(
        baseRDD(),
        options)
    }
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val requiredFields = requiredColumns.map(schema(_))
    val requestedSchema = StructType(requiredFields)
    StaxXmlParser.parse(
      baseRDD(),
      requestedSchema,
      options)
  }

  // The function below was borrowed from JSONRelation
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    if (locations.isEmpty) {
      throw new IOException(s"Cannot INSERT into table with no path defined")
    }
    if (locations.length > 1) {
      throw new IllegalArgumentException("Only one path may be specified for writing")
    }
    val filesystemPath = new Path(locations.head)

    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)

    if (overwrite) {
      try {
        fs.delete(filesystemPath, true)
      } catch {
        case e: IOException =>
          throw new IOException(
            s"Unable to clear output directory ${filesystemPath.toString} prior"
              + s" to INSERT OVERWRITE a XML table:\n${e.toString}")
      }
      // Write the data. We assume that schema isn't changed, and we won't update it.
      XmlFile.saveAsXmlFile(data, filesystemPath.toString, parameters)
    } else {
      throw new IllegalArgumentException("XML tables only support INSERT OVERWRITE for now.")
    }
  }
}
