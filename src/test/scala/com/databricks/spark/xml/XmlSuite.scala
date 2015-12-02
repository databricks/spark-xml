/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.databricks.spark.xml

import java.nio.charset.UnsupportedCharsetException

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._

abstract class AbstractXmlSuite extends FunSuite with BeforeAndAfterAll {
  val agesFile = "src/test/resources/ages.xml"
  val agesFileTag = "ROW"

  val agesAttributeFile = "src/test/resources/ages-attribute.xml"
  val agesAttributeFileTag = "ROW"

  val booksFile = "src/test/resources/books.xml"
  val booksFileTag = "book"

  val booksNestedObjectFile = "src/test/resources/books-nested-object.xml"
  val booksNestedObjectFileTag = "book"

  val booksNestedArrayFile = "src/test/resources/books-nested-array.xml"
  val booksNestedArrayFileTag = "book"

  val booksComplicatedFile = "src/test/resources/books-complicated.xml"
  val booksComplicatedFileTag = "book"

  val carsFile = "src/test/resources/cars.xml"
  val carsFileTag = "ROW"

  val carsUnbalancedFile = "src/test/resources/cars-unbalanced-elements.xml"
  val carsUnbalancedFileTag = "ROW"

  val nullNumbersFile = "src/test/resources/null-numbers.xml"
  val nullNumbersFileTag = "ROW"

  val emptyFile = "src/test/resources/empty.xml"
  val emptyFileTag = "ROW"

  val numCars = 3
  val numBooks = 12
  val numBooksComplicated = 3

  private var sqlContext: SQLContext = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(new SparkContext("local[2]", "AvroSuite"))
  }

  override protected def afterAll(): Unit = {
    try {
      sqlContext.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }

  test("DSL test") {
    val results = sqlContext
      .xmlFile(carsFile, rootTag = carsFileTag)
      .select("year")
      .collect()

    assert(results.size === numCars)
  }

  test("DSL test bad charset name") {
    val exception = intercept[UnsupportedCharsetException] {
      val results = sqlContext
        .xmlFile(carsFile, rootTag = carsFileTag, charset = "1-9588-osi")
        .select("year")
        .collect()
    }
    assert(exception.getMessage.contains("1-9588-osi"))
  }

  test("DDL test") {
    sqlContext.sql(
      s"""
         |CREATE TEMPORARY TABLE carsTable
         |USING com.databricks.spark.xml
         |OPTIONS (path "$carsFile", rootTag "$carsFileTag")
      """.stripMargin.replaceAll("\n", " "))

    assert(sqlContext.sql("SELECT year FROM carsTable").collect().size === numCars)
  }

  test("DDL test with alias name") {
    assume(org.apache.spark.SPARK_VERSION.take(3) >= "1.5",
      "Datasource alias feature was added in Spark 1.5")

    sqlContext.sql(
      s"""
         |CREATE TEMPORARY TABLE carsTable
         |USING xml
         |OPTIONS (path "$carsFile", rootTag "$carsFileTag")
      """.stripMargin.replaceAll("\n", " "))

    assert(sqlContext.sql("SELECT year FROM carsTable").collect().size === numCars)
  }

//   TODO: We need to support mode
//  test("DSL test for DROPMALFORMED parsing mode") {
//    val results = new XmlReader()
//      .withParseMode(ParseModes.DROP_MALFORMED_MODE)
//      .withUseHeader(true)
//      .withParserLib(parserLib)
//      .xmlFile(sqlContext, carsFile)
//      .select("year")
//      .collect()
//
//    assert(results.size === numCars - 1)
//  }

  // TODO: We need to support mode
//  test("DSL test for FAILFAST parsing mode") {
//    val parser = new XmlReader()
//      .withParseMode(ParseModes.FAIL_FAST_MODE)
//      .withUseHeader(true)
//      .withParserLib(parserLib)
//
//    val exception = intercept[SparkException]{
//      parser.xmlFile(sqlContext, carsFile)
//        .select("year")
//        .collect()
//    }
//
//    assert(exception.getMessage.contains("Malformed line in FAILFAST mode: 2015,Chevy,Volt"))
//  }


  test("DSL test with empty file and known schema") {
    val results = new XmlReader()
      .withSchema(StructType(List(StructField("column", StringType, false))))
      .withRootTag(emptyFileTag)
      .xmlFile(sqlContext, emptyFile)
      .count()

    assert(results === 0)
  }

  test("DSL test with poorly formatted file and string schema") {
    val stringSchema = new StructType(
      Array(
        StructField("year", LongType, true),
        StructField("make", StringType, true),
        StructField("model", StringType, true),
        StructField("comment", StringType, true)
      )
    )
    val results = new XmlReader()
      .withSchema(stringSchema)
      .withRootTag(carsUnbalancedFileTag)
      .xmlFile(sqlContext, carsUnbalancedFile)
      .count()

    assert(results === 3)
  }

  test("DDL test with empty file") {
    sqlContext.sql(s"""
           |CREATE TEMPORARY TABLE carsTable
           |(year double, make string, model string, comments string, grp string)
           |USING com.databricks.spark.xml
           |OPTIONS (path "$emptyFile", rootTag "$emptyFileTag")
      """.stripMargin.replaceAll("\n", " "))

    assert(sqlContext.sql("SELECT count(*) FROM carsTable").collect().head(0) === 0)
  }

  test("DSL test schema inferred correctly") {
    val results = sqlContext
      .xmlFile(booksFile, rootTag = booksFileTag)

    assert(results.schema == StructType(List(
      StructField("author", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("genre", StringType, nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("price", DoubleType, nullable = true),
      StructField("publish_date", StringType, nullable = true),
      StructField("title", StringType, nullable = true))
    ))

    assert(results.collect().size === numBooks)
  }

  test("DSL test schema (object) inferred correctly") {
    val results = sqlContext
      .xmlFile(booksNestedObjectFile, rootTag = booksNestedObjectFileTag)

    assert(results.schema == StructType(List(
      StructField("author", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("genre", StringType, nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("price", DoubleType, nullable = true),
      StructField("publish_dates", StructType(
        List(StructField("publish_date", StringType))), nullable = true),
      StructField("title", StringType, nullable = true))
    ))

    assert(results.collect().size === numBooks)
  }

  test("DSL test schema (array) inferred correctly") {
    val results = sqlContext
      .xmlFile(booksNestedArrayFile, rootTag = booksNestedArrayFileTag)

    assert(results.schema == StructType(List(
      StructField("author", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("genre", StringType, nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("price", DoubleType, nullable = true),
      StructField("publish_date", ArrayType(StringType), nullable = true),
      StructField("title", StringType, nullable = true))
    ))

    assert(results.collect().size === numBooks)
  }

  test("DSL test schema (complicated) inferred correctly") {
    val results = sqlContext
      .xmlFile(booksComplicatedFile, rootTag = booksComplicatedFileTag)

    assert(results.schema == StructType(List(
      StructField("author", StringType, nullable = true),
      StructField("genre", StructType(
        List(StructField("genreid", LongType),
          StructField("name", StringType))),
        nullable = true),
      StructField("id", StringType, nullable = true),
      StructField("price", DoubleType, nullable = true),
      StructField("publish_dates", StructType(
        List(StructField("publish_date",
            ArrayType(StructType(
                List(StructField("day", LongType, nullable = true),
                  StructField("month", LongType, nullable = true),
                  StructField("tag", StringType, nullable = true),
                  StructField("year", LongType, nullable = true))))))),
        nullable = true),
      StructField("title", StringType, nullable = true))
    ))

    assert(results.collect().size === numBooksComplicated)
  }

  test("DSL test with different data types") {
    val stringSchema = new StructType(
      Array(
        StructField("year", IntegerType, true),
        StructField("make", StringType, true),
        StructField("model", StringType, true),
        StructField("comment", StringType, true)
      )
    )
    val results = new XmlReader()
      .withSchema(stringSchema)
      .withRootTag(carsUnbalancedFileTag)
      .xmlFile(sqlContext, carsUnbalancedFile)
      .count()

    assert(results === 3)
  }

  test("DSL test inferred schema passed through") {
    val dataFrame = sqlContext
      .xmlFile(carsFile, rootTag = carsFileTag)

    val results = dataFrame
      .select("comment", "year")
      .where(dataFrame("year") === 2012)

    assert(results.first.getString(0) === "No comment")
    assert(results.first.getLong(1) === 2012)
  }

  test("DSL test nullable fields") {
    val results = new XmlReader()
      .withSchema(StructType(List(StructField("name", StringType, false),
                                  StructField("age", IntegerType, true))))
      .withRootTag(nullNumbersFileTag)
      .xmlFile(sqlContext, nullNumbersFile)
      .collect()

    assert(results.head.toSeq === Seq("alice", 35))
    assert(results(1).toSeq === Seq("bob", null))
    assert(results(2).toSeq === Seq("coc", 24))
  }
}

class XmlSuite extends AbstractXmlSuite {
}
