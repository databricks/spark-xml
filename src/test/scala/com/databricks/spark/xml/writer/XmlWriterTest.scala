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
package com.databricks.spark.xml.writer

import java.io.File
import java.nio.charset.StandardCharsets

import com.databricks.spark.xml.util.XmlFile
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

final class XmlWriterTest extends FunSuite with BeforeAndAfterAll {

  private var sparkContext: SparkContext = _
  val spark: SparkSession = SparkSession.builder.appName("XmlWriterTest").master("local[1]")
    .getOrCreate()
  override def beforeAll(): Unit = {
    super.beforeAll()

    sparkContext = spark.sparkContext

  }

  override def afterAll(): Unit = {
    try {
      sparkContext.stop()
      sparkContext = null
    } finally {
      super.afterAll()
    }
  }

  def deleteFiles(path: String): Unit = {
    val file = new File(path)
    if (!file.exists) return
    if (file.isFile) file.delete
    else {
      val subFiles = file.listFiles
      for (subfile <- subFiles) {
        deleteFiles(subfile.getAbsolutePath)
      }
      file.delete
    }
  }

  test("test level2 self close tag") {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val singersDF = Seq(
      ("ahuoo", "www.ahuoo.com"),
      ("google", "www.google.com"),
      ("facebook", null)
    ).toDF("_name", "_url")
    singersDF.show()
    val outputFolder = "test2"
    deleteFiles(outputFolder)
    singersDF.printSchema()
    var  params = Map("version"-> "2.0", "selfCloseTag"->"true", "nullValue"->"++")
    XmlFile.saveAsXmlFile(singersDF, outputFolder, params)
  }

  test("test level3 self close tag") {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val singersDF = Seq(
      ("beatles", "help|hey jude"),
      ("romeo", "eres mia"),
      ("romeo", null)
    ).toDF("_name", "hit_songs")

    val actualDF = singersDF
      .withColumn("hit_songs", split(col("hit_songs"), "\\|"))
      .withColumn("selfcloseTag", struct("_name"))
      .withColumn("some_array", typedLit(Seq(1, 2, 3)))
      .withColumn("some_struct", typedLit(("foo", 1, 0.3)))
      .withColumn(
        "some_struct2",
        struct(lit("foo"), lit(1), lit(0.3)).cast("struct<_x: string, _y: integer, _VALUE: double>")
      )
      .withColumn("some_map", typedLit(Map("_key1" -> 1, "_key2" -> 2)))
    actualDF.show()
    val outputFolder = "test3"
    deleteFiles(outputFolder)
    actualDF.printSchema()
    var  params = Map("version"-> "2.0", "selfCloseTag"->"true", "nullValue"->"++")
    XmlFile.saveAsXmlFile(actualDF, outputFolder, params)
  }


}
