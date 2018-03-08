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
package com.databricks.spark.xml.util

import java.nio.charset.UnsupportedCharsetException

import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class XmlFileSuite extends FunSuite with BeforeAndAfterAll {
  val booksFile = "src/test/resources/books.xml"
  val booksFileTag = "book"

  val numBooks = 12

  val fiasHouse = "src/test/resources/fias_house.xml"
  val fiasRowTag = "House"
  val numHouses = 37

  val utf8 = "utf-8"

  private var sparkContext: SparkContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext = new SparkContext("local[2]", "TextFileSuite")
  }

  override def afterAll(): Unit = {
    try {
      sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }

  test("read utf-8 encoded file") {
    val baseRDD = XmlFile.withCharset(sparkContext, booksFile, utf8, rowTag = booksFileTag)
    assert(baseRDD.count() === numBooks)
  }

  test("read utf-8 encoded file with empty tag") {
    val baseRDD = XmlFile.withCharset(sparkContext, fiasHouse, utf8, rowTag = fiasRowTag)
    assert(baseRDD.count() == numHouses)
    baseRDD.collect().foreach(x => assert(x.contains("/>")))
  }

  test("unsupported charset") {
    val exception = intercept[UnsupportedCharsetException] {
      XmlFile.withCharset(sparkContext, booksFile, "frylock", rowTag = booksFileTag).count()
    }
    assert(exception.getMessage.contains("frylock"))
  }
}
