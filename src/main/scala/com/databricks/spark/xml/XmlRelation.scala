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
import org.slf4j.LoggerFactory

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{PrunedScan, InsertableRelation, BaseRelation, TableScan}
import org.apache.spark.sql.types._
import com.databricks.spark.xml.util.InferSchema
import com.databricks.spark.xml.parsers.{StaxXmlParser, StaxConfiguration}

case class XmlRelation protected[spark] (
    baseRDD: () => RDD[String],
    location: Option[String],
    samplingRatio: Double,
    excludeAttributeFlag: Boolean,
    treatEmptyValuesAsNulls: Boolean,
    failFastFlag: Boolean,
    attributePrefix: String,
    valueTag: String,
    userSchema: StructType = null)(@transient val sqlContext: SQLContext)
  extends BaseRelation
  with InsertableRelation
  with TableScan
  with PrunedScan {

  private val logger = LoggerFactory.getLogger(XmlRelation.getClass)

  private val parseConf = StaxConfiguration(
    samplingRatio,
    excludeAttributeFlag,
    treatEmptyValuesAsNulls,
    failFastFlag,
    attributePrefix,
    valueTag
  )

  override val schema: StructType = {
    Option(userSchema).getOrElse {
      InferSchema.infer(
        baseRDD(),
        parseConf)
    }
  }

  override def buildScan(): RDD[Row] = {
    StaxXmlParser.parse(
      baseRDD(),
      schema,
      parseConf)
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val requiredFields = requiredColumns.map(schema(_))
    val schemaFields = schema.fields
    if (schemaFields.deep == requiredFields.deep) {
      buildScan()
    } else if (parseConf.failFastFlag) {
      val safeRequestedSchema = StructType(requiredFields)
      StaxXmlParser.parse(
        baseRDD(),
        safeRequestedSchema,
        parseConf)
    } else {
      // If `failFast` is disabled, then it needs to parse all the values
      // so that we can decide which row is malformed.
      val safeRequestedSchema = StructType(
        requiredFields ++ schema.fields.filterNot(requiredFields.contains(_)))
      val rows = StaxXmlParser.parse(
        baseRDD(),
        safeRequestedSchema,
        parseConf)

      val rowSize = requiredFields.length
      rows.mapPartitions { iter =>
        iter.flatMap { xml =>
          Some(Row.fromSeq(xml.toSeq.take(rowSize)))
        }
      }
    }
  }

  // The function below was borrowed from JSONRelation
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val filesystemPath = location match {
      case Some(p) => new Path(p)
      case None =>
        throw new IOException(s"Cannot INSERT into table with no path defined")
    }

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
      data.saveAsXmlFile(filesystemPath.toString)
    } else {
      sys.error("XML tables only support INSERT OVERWRITE for now.")
    }
  }
}
