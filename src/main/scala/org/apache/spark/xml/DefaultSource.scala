/*
 * Copyright 2014 Apache
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
package org.apache.spark.xml

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.xml.util.TextFile

/**
 * Provides access to XML data from pure SQL statements (i.e. for users of the
 * JDBC server).
 */
class DefaultSource
  extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {

  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path", sys.error("'path' must be specified for XML data."))
  }

  /**
   * Creates a new relation for data store in XML given parameters.
   * Parameters have to include 'path'.
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  /**
   * Creates a new relation for data store in XML given parameters and user supported schema.
   * Parameters have to include 'path'.
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): XmlRelation = {
    val path = checkPath(parameters)
    val samplingRatio = parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)

    val parseMode = parameters.getOrElse("mode", "PERMISSIVE")

    val comment = parameters.getOrElse("comment", "false")
    val commentFlag = if (comment == "false") {
      false
    } else if (comment == "true") {
      true
    } else {
      throw new Exception("Ignore comment flag can be true or false")
    }
    val ignoreAttribute = parameters.getOrElse("ignoreAttribute", "false")
    val ignoreAttributeFlag = if (comment == "false") {
      false
    } else if (comment == "true") {
      true
    } else {
      throw new Exception("Ignore attribute flag can be true or false")
    }
    val charset = parameters.getOrElse("charset", TextFile.DEFAULT_CHARSET.name())
    // TODO validate charset?

    XmlRelation(
      () => TextFile.withCharset(sqlContext.sparkContext, path, charset),
      Some(path),
      parseMode,
      samplingRatio,
      commentFlag,
      ignoreAttributeFlag,
      schema)(sqlContext)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val path = checkPath(parameters)
    val filesystemPath = new Path(path)
    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
    val doSave = if (fs.exists(filesystemPath)) {
      mode match {
        case SaveMode.Append =>
          sys.error(s"Append mode is not supported by ${this.getClass.getCanonicalName}")
        case SaveMode.Overwrite =>
          fs.delete(filesystemPath, true)
          true
        case SaveMode.ErrorIfExists =>
          sys.error(s"path $path already exists.")
        case SaveMode.Ignore => false
      }
    } else {
      true
    }
    if (doSave) {
      // Only save data when the save mode is not ignore.
      data.saveAsXmlFile(path, parameters)
    }

    createRelation(sqlContext, parameters, data.schema)
  }
}
