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

import java.io.File
import java.nio.charset.UnsupportedCharsetException
import java.sql.{Date, Timestamp}

import scala.io.Source

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.GzipCodec

import org.apache.spark.{SparkException, SparkContext}
import org.apache.spark.sql.{SaveMode, Row, SQLContext}
import org.apache.spark.sql.types._
import com.databricks.spark.xml.XmlOptions._
import com.databricks.spark.xml.util.ParseModes

class XmlSuite extends FunSuite with BeforeAndAfterAll {
  val tempEmptyDir = "target/test/empty/"
  val agesFile = "src/test/resources/ages.xml"
  val booksFile = "src/test/resources/books.xml"
  val booksNestedObjectFile = "src/test/resources/books-nested-object.xml"
  val booksNestedArrayFile = "src/test/resources/books-nested-array.xml"
  val booksComplicatedFile = "src/test/resources/books-complicated.xml"
  val carsFile = "src/test/resources/cars.xml"
  val carsFile8859 = "src/test/resources/cars-iso-8859-1.xml"
  val carsFileGzip = "src/test/resources/cars.xml.gz"
  val carsFileBzip2 = "src/test/resources/cars.xml.bz2"
  val carsMixedAttrNoChildFile = "src/test/resources/cars-mixed-attr-no-child.xml"
  val booksAttributesInNoChild = "src/test/resources/books-attributes-in-no-child.xml"
  val carsUnbalancedFile = "src/test/resources/cars-unbalanced-elements.xml"
  val carsMalformedFile = "src/test/resources/cars-malformed.xml"
  val nullNumbersFile = "src/test/resources/null-numbers.xml"
  val emptyFile = "src/test/resources/empty.xml"
  val topicsFile = "src/test/resources/topics-namespaces.xml"
  val gpsEmptyField = "src/test/resources/gps-empty-field.xml"
  val agesMixedTypes = "src/test/resources/ages-mixed-types.xml"
  val nullNestedStructFile = "src/test/resources/null-nested-struct.xml"
  val simpleNestedObjects = "src/test/resources/simple-nested-objects.xml"
  val nestedElementWithNameOfParent = "src/test/resources/nested-element-with-name-of-parent.xml"

  val booksTag = "book"
  val booksRootTag = "books"
  val topicsTag = "Topic"
  val agesTag = "person"

  val numAges = 3
  val numCars = 3
  val numBooks = 12
  val numBooksComplicated = 3
  val numTopics = 1
  val numGPS = 2

  private var sqlContext: SQLContext = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(new SparkContext("local[2]", "XmlSuite"))
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
      .xmlFile(carsFile)
      .select("year")
      .collect()

    assert(results.size === numCars)
  }

  test("DSL test with xml having unbalanced datatypes") {
    val results = sqlContext
      .xmlFile(gpsEmptyField, treatEmptyValuesAsNulls = true)

    assert(results.collect().size === numGPS)
  }

  test("DSL test with mixed elements (attributes, no child)") {
    val results = sqlContext
      .xmlFile(carsMixedAttrNoChildFile)
      .select("date")
      .collect()

    val attrValOne = results(0).get(0).asInstanceOf[Row](1)
    val attrValTwo = results(1).get(0).asInstanceOf[Row](1)
    assert(attrValOne == "string")
    assert(attrValTwo == "struct")
    assert(results.size === numCars)
  }

  test("DSL test for inconsistent element attributes as fields") {
    val results = sqlContext
      .xmlFile(booksAttributesInNoChild, rowTag = booksTag)
      .select("price")

    // This should not throw an exception `java.lang.ArrayIndexOutOfBoundsException`
    // as non-existing values are represented as `null`s.
    val nullValue = results
      .collect()
      .toSeq.head
      .toSeq.head
      .asInstanceOf[Row]
      .get(1)
    assert(nullValue == null)
  }

  test("DSL test with mixed elements (struct, string)") {
    val results = sqlContext
      .xmlFile(agesMixedTypes, rowTag = agesTag).collect()
    assert(results.size === numAges)
  }

  test("DSL test with elements in array having attributes") {
    val results = sqlContext.xmlFile(agesFile, rowTag = agesTag).collect()
    val attrValOne = results(0).get(0).asInstanceOf[Row](1)
    val attrValTwo = results(1).get(0).asInstanceOf[Row](1)
    assert(attrValOne == "1990-02-24")
    assert(attrValTwo == "1985-01-01")
    assert(results.size === numAges)
  }

  test("DSL test for iso-8859-1 encoded file") {
    val dataFrame = new XmlReader()
      .withCharset("iso-8859-1")
      .xmlFile(sqlContext, carsFile8859)
    assert(dataFrame.select("year").collect().size === numCars)

    val results = dataFrame
      .select("comment", "year")
      .where(dataFrame("year") === 2012)
    assert(results.first.getString(0) === "No comment")
    assert(results.first.getLong(1) === 2012)
  }

  test("DSL test compressed file") {
    val results = sqlContext
      .xmlFile(carsFileGzip)
      .select("year")
      .collect()

    assert(results.size === numCars)
  }

  test("DSL test splittable compressed file") {
    val results = sqlContext
      .xmlFile(carsFileBzip2)
      .select("year")
      .collect()

    assert(results.size === numCars)
  }

  test("DSL test bad charset name") {
    val exception = intercept[UnsupportedCharsetException] {
      val results = sqlContext
        .xmlFile(carsFile, charset = "1-9588-osi")
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
         |OPTIONS (path "$carsFile")
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
         |OPTIONS (path "$carsFile")
      """.stripMargin.replaceAll("\n", " "))

    assert(sqlContext.sql("SELECT year FROM carsTable").collect().size === numCars)
  }

  test("DSL test for parsing a malformed XML file") {
    val results = new XmlReader()
      .withParseMode(ParseModes.DROP_MALFORMED_MODE)
      .xmlFile(sqlContext, carsMalformedFile)

    assert(results.count() === 1)
  }

  test("DSL test for dropping malformed rows") {
    val cars = new XmlReader()
      .withParseMode(ParseModes.DROP_MALFORMED_MODE)
      .xmlFile(sqlContext, carsMalformedFile)

    assert(cars.count() == 1)
    assert(cars.head().toSeq === Seq("Chevy", "Volt", 2015))
  }

  test("DSL test for failing fast") {
    val exceptionInParse = intercept[SparkException] {
      new XmlReader()
        .withFailFast(true)
        .xmlFile(sqlContext, carsMalformedFile)
        .collect()
    }
    assert(exceptionInParse.getMessage.contains("Malformed line in FAILFAST mode"))
  }

  test("DSL test for permissive mode for corrupt records") {
    val cars = new XmlReader()
      .withParseMode(ParseModes.PERMISSIVE_MODE)
      .xmlFile(sqlContext, carsMalformedFile)
      .collect()
    assert(cars.length == 3)

    val malformedRowOne = cars(0).toSeq.head.toString
    val malformedRowTwo = cars(1).toSeq.head.toString
    val expectedMalformedRowOne = "<ROW><year>2012</year><make>Tesla</make><model>>S" +
      "<comment>No comment</comment></ROW>"
    val expectedMalformedRowTwo = "<ROW></year><make>Ford</make><model>E350</model>model></model>" +
      "<comment>Go get one now they are going fast</comment></ROW>"

    assert(malformedRowOne.replaceAll("\\s", "") === expectedMalformedRowOne.replaceAll("\\s", ""))
    assert(malformedRowTwo.replaceAll("\\s", "") === expectedMalformedRowTwo.replaceAll("\\s", ""))
    assert(cars(2).toSeq.head === null)
    assert(cars(0).toSeq.takeRight(3) === Seq(null, null, null))
    assert(cars(1).toSeq.takeRight(3) === Seq(null, null, null))
    assert(cars(2).toSeq.takeRight(3) === Seq("Chevy", "Volt", 2015))
  }

  test("DSL test with empty file and known schema") {
    val results = new XmlReader()
      .withSchema(StructType(List(StructField("column", StringType, false))))
      .xmlFile(sqlContext, emptyFile)
      .count()

    assert(results === 0)
  }

  test("DSL test with poorly formatted file and string schema") {
    val schema = new StructType(
      Array(
        StructField("color", StringType, true),
        StructField("year", StringType, true),
        StructField("make", StringType, true),
        StructField("model", StringType, true),
        StructField("comment", StringType, true)
      )
    )
    val results = new XmlReader()
      .withSchema(schema)
      .xmlFile(sqlContext, carsUnbalancedFile)
      .count()

    assert(results === numCars)
  }

  test("DDL test with empty file") {
    sqlContext.sql(s"""
           |CREATE TEMPORARY TABLE carsTable
           |(year double, make string, model string, comments string, grp string)
           |USING com.databricks.spark.xml
           |OPTIONS (path "$emptyFile")
      """.stripMargin.replaceAll("\n", " "))

    assert(sqlContext.sql("SELECT count(*) FROM carsTable").collect().head(0) === 0)
  }

  test("SQL test insert overwrite") {
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    sqlContext.sql(
      s"""
         |CREATE TEMPORARY TABLE booksTableIO
         |USING com.databricks.spark.xml
         |OPTIONS (path "$booksFile", rowTag "$booksTag")
      """.stripMargin.replaceAll("\n", " "))
    sqlContext.sql(
      s"""
         |CREATE TEMPORARY TABLE booksTableEmpty
         |(author string, description string, genre string,
         |id string, price double, publish_date string, title string)
         |USING com.databricks.spark.xml
         |OPTIONS (path "$tempEmptyDir")
      """.stripMargin.replaceAll("\n", " "))

    assert(sqlContext.sql("SELECT * FROM booksTableIO").collect().size === numBooks)
    assert(sqlContext.sql("SELECT * FROM booksTableEmpty").collect().isEmpty)

    sqlContext.sql(
      s"""
         |INSERT OVERWRITE TABLE booksTableEmpty
         |SELECT * FROM booksTableIO
      """.stripMargin.replaceAll("\n", " "))
    assert(sqlContext.sql("SELECT * FROM booksTableEmpty").collect().size == numBooks)
  }

  test("DSL save with gzip compression codec") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "cars-copy.xml"

    val cars = sqlContext.xmlFile(carsFile)
    cars.save("com.databricks.spark.xml", SaveMode.Overwrite,
      Map("path" -> copyFilePath, "codec" -> classOf[GzipCodec].getName))
    val carsCopyPartFile = new File(copyFilePath, "part-00000.gz")
    // Check that the part file has a .gz extension
    assert(carsCopyPartFile.exists())

    val carsCopy = sqlContext.xmlFile(copyFilePath + "/")

    assert(carsCopy.count == cars.count)
    assert(carsCopy.collect.map(_.toString).toSet == cars.collect.map(_.toString).toSet)
  }

  test("DSL save with gzip compression codec by shorten name") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "cars-copy.xml"

    val cars = sqlContext.xmlFile(carsFile)
    cars.save("com.databricks.spark.xml", SaveMode.Overwrite,
      Map("path" -> copyFilePath, "compression" -> "gZiP"))
    val carsCopyPartFile = new File(copyFilePath, "part-00000.gz")
    // Check that the part file has a .gz extension
    assert(carsCopyPartFile.exists())

    val carsCopy = sqlContext.xmlFile(copyFilePath)

    assert(carsCopy.count == cars.count)
    assert(carsCopy.collect.map(_.toString).toSet == cars.collect.map(_.toString).toSet)
  }

  test("DSL save") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "books-copy.xml"

    val books = sqlContext.xmlFile(booksComplicatedFile, rowTag = booksTag)
    books.saveAsXmlFile(copyFilePath,
      Map("rootTag" -> booksRootTag, "rowTag" -> booksTag))

    val booksCopy = sqlContext.xmlFile(copyFilePath + "/", rowTag = booksTag)
    assert(booksCopy.count == books.count)
    assert(booksCopy.collect.map(_.toString).toSet === books.collect.map(_.toString).toSet)
  }

  test("DSL save with nullValue and treatEmptyValuesAsNulls") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "books-copy.xml"

    val books = sqlContext.xmlFile(booksComplicatedFile, rowTag = booksTag)
    books.saveAsXmlFile(copyFilePath,
      Map("rootTag" -> booksRootTag, "rowTag" -> booksTag, "nullValue" -> ""))

    val booksCopy =
      sqlContext.xmlFile(copyFilePath, rowTag = booksTag, treatEmptyValuesAsNulls = true)

    assert(booksCopy.count == books.count)
    assert(booksCopy.collect.map(_.toString).toSet === books.collect.map(_.toString).toSet)
  }

  test("DSL save dataframe not read from a XML file") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "data-copy.xml"

    val schema = StructType(
      List(StructField("a", ArrayType(ArrayType(StringType)), nullable = true)))
    val data = sqlContext.sparkContext.parallelize(
      List(List(List("aa", "bb"), List("aa", "bb")))).map(Row(_))
    val df = sqlContext.createDataFrame(data, schema)
    df.saveAsXmlFile(copyFilePath)

    // When [[ArrayType]] has [[ArrayType]] as elements, it is confusing what is the element
    // name for XML file. Now, it is "item". So, "item" field is additionally added
    // to wrap the element.
    val schemaCopy = StructType(
      List(StructField("a", ArrayType(
        StructType(List(StructField("item", ArrayType(StringType), nullable = true)))),
        nullable = true)))
    val dfCopy = sqlContext.xmlFile(copyFilePath + "/")

    assert(dfCopy.count == df.count)
    assert(dfCopy.schema === schemaCopy)
  }

  test("DSL save dataframe with data types correctly") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "data-copy.xml"

    // Create the schema.
    val dataTypes =
      Seq(
        StringType, NullType, BooleanType,
        ByteType, ShortType, IntegerType, LongType,
        FloatType, DoubleType, DecimalType(25, 3), DecimalType(6, 5),
        DateType, TimestampType, MapType(StringType, StringType))
    val fields = dataTypes.zipWithIndex.map { case (dataType, index) =>
      StructField(s"col$index", dataType, nullable = true)
    }
    val schema = StructType(fields)

    // Create the data
    val timestamp = "2015-01-01 00:00:00"
    val date = "2015-01-01"
    val row =
      Row(
        "aa", null, true,
        1.toByte, 1.toShort, 1, 1.toLong,
        1.toFloat, 1.toDouble, Decimal(1, 25, 3), Decimal(1, 6, 5),
        Date.valueOf(date), Timestamp.valueOf(timestamp), Map("a" -> "b"))
    val data = sqlContext.sparkContext.parallelize(Seq(row))

    val df = sqlContext.createDataFrame(data, schema)
    df.saveAsXmlFile(copyFilePath)

    val dfCopy = new XmlReader()
      .withSchema(schema)
      .xmlFile(sqlContext, copyFilePath + "/")

    assert(dfCopy.collect() === df.collect())
    assert(dfCopy.schema === df.schema)
  }

  test("DSL test schema inferred correctly") {
    val results = sqlContext
      .xmlFile(booksFile, rowTag = booksTag)

    assert(results.schema == StructType(List(
      StructField(s"${DEFAULT_ATTRIBUTE_PREFIX}id", StringType, nullable = true),
      StructField("author", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("genre", StringType, nullable = true),
      StructField("price", DoubleType, nullable = true),
      StructField("publish_date", StringType, nullable = true),
      StructField("title", StringType, nullable = true))
    ))

    assert(results.collect().size === numBooks)
  }

  test("DSL test schema inferred correctly with sampling ratio") {
    val results = sqlContext
      .xmlFile(booksFile, rowTag = booksTag, samplingRatio = 0.5)

    assert(results.schema == StructType(List(
      StructField(s"${DEFAULT_ATTRIBUTE_PREFIX}id", StringType, nullable = true),
      StructField("author", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("genre", StringType, nullable = true),
      StructField("price", DoubleType, nullable = true),
      StructField("publish_date", StringType, nullable = true),
      StructField("title", StringType, nullable = true))
    ))

    assert(results.collect().size === numBooks)
  }

  test("DSL test schema (object) inferred correctly") {
    val results = sqlContext
      .xmlFile(booksNestedObjectFile, rowTag = booksTag)

    assert(results.schema == StructType(List(
      StructField(s"${DEFAULT_ATTRIBUTE_PREFIX}id", StringType, nullable = true),
      StructField("author", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("genre", StringType, nullable = true),
      StructField("price", DoubleType, nullable = true),
      StructField("publish_dates", StructType(
        List(StructField("publish_date", StringType))), nullable = true),
      StructField("title", StringType, nullable = true))
    ))

    assert(results.collect().size === numBooks)
  }

  test("DSL test schema (array) inferred correctly") {
    val results = sqlContext
      .xmlFile(booksNestedArrayFile, rowTag = booksTag)

    assert(results.schema == StructType(List(
      StructField(s"${DEFAULT_ATTRIBUTE_PREFIX}id", StringType, nullable = true),
      StructField("author", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("genre", StringType, nullable = true),
      StructField("price", DoubleType, nullable = true),
      StructField("publish_date", ArrayType(StringType), nullable = true),
      StructField("title", StringType, nullable = true))
    ))

    assert(results.collect().size === numBooks)
  }

  test("DSL test schema (complicated) inferred correctly") {
    val results = sqlContext
      .xmlFile(booksComplicatedFile, rowTag = booksTag)

    assert(results.schema == StructType(List(
      StructField(s"${DEFAULT_ATTRIBUTE_PREFIX}id", StringType, nullable = true),
      StructField("author", StringType, nullable = true),
      StructField("genre", StructType(
        List(StructField("genreid", LongType),
          StructField("name", StringType))),
        nullable = true),
      StructField("price", DoubleType, nullable = true),
      StructField("publish_dates", StructType(
        List(StructField("publish_date",
          ArrayType(StructType(
            List(StructField(s"${DEFAULT_ATTRIBUTE_PREFIX}tag", StringType, nullable = true),
              StructField("day", LongType, nullable = true),
              StructField("month", LongType, nullable = true),
              StructField("year", LongType, nullable = true))))))),
        nullable = true),
      StructField("title", StringType, nullable = true))
    ))

    assert(results.collect().size === numBooksComplicated)
  }

  test("DSL test parsing and inferring attribute in elements having no child element") {
    // Default value.
    val resultsOne = new XmlReader()
      .withRowTag(booksTag)
      .xmlFile(sqlContext, booksAttributesInNoChild)

    val schemaOne = StructType(List(
      StructField("_id", StringType, nullable = true),
      StructField("author", StringType, nullable = true),
      StructField("price", StructType(
        List(StructField("_VALUE", StringType, nullable = true),
          StructField(s"_unit", StringType, nullable = true))),
        nullable = true),
      StructField("publish_date", StringType, nullable = true),
      StructField("title", StringType, nullable = true))
    )

    assert(resultsOne.schema === schemaOne)
    assert(resultsOne.count == numBooks)

    // Explicitly set
    val attributePrefix = "@#"
    val valueTag = "#@@value"
    val resultsTwo = new XmlReader()
      .withRowTag(booksTag)
      .withAttributePrefix(attributePrefix)
      .withValueTag(valueTag)
      .xmlFile(sqlContext, booksAttributesInNoChild)

    val schemaTwo = StructType(List(
      StructField(s"${attributePrefix}id", StringType, nullable = true),
      StructField("author", StringType, nullable = true),
      StructField("price", StructType(
        List(StructField(valueTag, StringType, nullable = true),
          StructField(s"${attributePrefix}unit", StringType, nullable = true))),
        nullable = true),
      StructField("publish_date", StringType, nullable = true),
      StructField("title", StringType, nullable = true))
    )

    assert(resultsTwo.schema === schemaTwo)
    assert(resultsTwo.count == numBooks)
  }

  test("DSL test schema (excluding tags) inferred correctly") {
    val results = new XmlReader()
      .withExcludeAttribute(true)
      .withRowTag(booksTag)
      .xmlFile(sqlContext, booksFile)

    val schema = StructType(List(
      StructField("author", StringType, nullable = true),
      StructField("description", StringType, nullable = true),
      StructField("genre", StringType, nullable = true),
      StructField("price", DoubleType, nullable = true),
      StructField("publish_date", StringType, nullable = true),
      StructField("title", StringType, nullable = true))
    )

    assert(results.schema === schema)
  }

  test("DSL test with custom schema") {
    val schema = new StructType(
      Array(
        StructField("make", StringType, true),
        StructField("model", StringType, true),
        StructField("comment", StringType, true),
        StructField("color", StringType, true),
        StructField("year", IntegerType, true)
      )
    )
    val results = new XmlReader()
      .withSchema(schema)
      .xmlFile(sqlContext, carsUnbalancedFile)
      .count()

    assert(results === numCars)
  }

  test("DSL test inferred schema passed through") {
    val dataFrame = sqlContext
      .xmlFile(carsFile)

    val results = dataFrame
      .select("comment", "year")
      .where(dataFrame("year") === 2012)

    assert(results.first.getString(0) === "No comment")
    assert(results.first.getLong(1) === 2012)
  }

  test("DSL test nullable fields") {
    val schema = StructType(
      StructField("name", StringType, false) ::
      StructField("age", StringType, true) :: Nil)
    val results = new XmlReader()
      .withSchema(schema)
      .xmlFile(sqlContext, nullNumbersFile)
      .collect()

    assert(results.head.toSeq === Seq("alice", "35"))
    assert(results(1).toSeq === Seq("bob", "    "))
    assert(results(2).toSeq === Seq("coc", "24"))
  }

  test("DSL test for treating empty string as null value") {
    val results = new XmlReader()
      .withSchema(StructType(List(StructField("name", StringType, false),
      StructField("age", IntegerType, true))))
      .withTreatEmptyValuesAsNulls(true)
      .xmlFile(sqlContext, nullNumbersFile)
      .collect()

    assert(results(1).toSeq === Seq("bob", null))
  }

  test("DSL test with namespaces ignored") {
    val results = sqlContext
      .xmlFile(topicsFile, rowTag = topicsTag)
      .collect()

    assert(results.size === numTopics)
  }

  test("Missing nested struct represented as null instead of empty Row") {
    val result = sqlContext
      .xmlFile(nullNestedStructFile, rowTag = "item")
      .select("b.es")
      .collect()

    assert(result(1).toSeq === Seq(null))
  }

  test("Produces correct order of columns for nested rows when user specifies a schema") {
    val nestedSchema = StructType(
      Seq(
        StructField("b", IntegerType, nullable = true),
        StructField("a", IntegerType, nullable = true)))
    val schema = StructType(
      Seq(
        StructField("c", nestedSchema, nullable = true)))

    val result = new XmlReader()
      .withSchema(schema)
      .xmlFile(sqlContext, simpleNestedObjects)
      .select("c.a", "c.b")
      .collect()

    assert(result(0).toSeq === Seq(111, 222))
  }

  test("Nested element with same name as parent delinination") {
    val lines = Source.fromFile(nestedElementWithNameOfParent).getLines.toList
    val firstExpected = lines(2).trim
    val lastExpected = lines(3).trim
    val config = new Configuration(sqlContext.sparkContext.hadoopConfiguration)
    config.set(XmlInputFormat.START_TAG_KEY, "<parent>")
    config.set(XmlInputFormat.END_TAG_KEY, "</parent>")
    config.set(XmlInputFormat.ENCODING_KEY, "utf-8")
    val records = sqlContext.sparkContext.newAPIHadoopFile(
      nestedElementWithNameOfParent,
      classOf[XmlInputFormat],
      classOf[LongWritable],
      classOf[Text],
      config)
    val list = records.values.map(t => t.toString).collect().toList
    assert(list.length == 2)
    val firstActual = list.head
    val lastActual = list.last
    assert(firstActual === firstExpected)
    assert(lastActual === lastExpected)
  }

  test("Nested element with same name as parent schema inference") {
    val df = new XmlReader()
      .withRowTag("parent")
      .xmlFile(sqlContext, nestedElementWithNameOfParent)

    val nestedSchema = StructType(
      Seq(
        StructField("child", StringType, nullable = true)))
    val schema = StructType(
      Seq(
        StructField("child", StringType, nullable = true),
        StructField("parent", nestedSchema, nullable = true)))
    df.schema.printTreeString()
    schema.printTreeString()
    assert(df.schema == schema)
  }

  test("Empty string not allowed for rowTag, attributePrefix and valueTag.") {
    val messageOne = intercept[IllegalArgumentException] {
      sqlContext.xmlFile(carsFile, rowTag = "").collect()
    }.getMessage
    assert(messageOne == "requirement failed: 'rowTag' option should not be empty string.")

    val messageTwo = intercept[IllegalArgumentException] {
      sqlContext.xmlFile(carsFile, attributePrefix = "").collect()
    }.getMessage
    assert(
      messageTwo == "requirement failed: 'attributePrefix' option should not be empty string.")

    val messageThree = intercept[IllegalArgumentException] {
      sqlContext.xmlFile(carsFile, valueTag = "").collect()
    }.getMessage
    assert(messageThree == "requirement failed: 'valueTag' option should not be empty string.")
  }
}
