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
import java.nio.file.Files
import java.sql.{Date, Timestamp}

import scala.io.Source

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.io.compress.GzipCodec
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import com.databricks.spark.xml.XmlOptions._
import com.databricks.spark.xml.util.ParseModes
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext, SparkException}

class XmlSuite extends FunSuite with BeforeAndAfterAll {
  val tempEmptyDir = "target/test/empty/"
  val agesFile = "src/test/resources/ages.xml"
  val agesWithSpacesFile = "src/test/resources/ages-with-spaces.xml"
  val booksFile = "src/test/resources/books.xml"
  val booksNestedObjectFile = "src/test/resources/books-nested-object.xml"
  val booksNestedArrayFile = "src/test/resources/books-nested-array.xml"
  val booksComplicatedFile = "src/test/resources/books-complicated.xml"
  val carsFile = "src/test/resources/cars.xml"
  val carsFile8859 = "src/test/resources/cars-iso-8859-1.xml"
  val carsFileGzip = "src/test/resources/cars.xml.gz"
  val carsFileBzip2 = "src/test/resources/cars.xml.bz2"
  val carsNoIndentationFile = "src/test/resources/cars-no-indentation.xml"
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
  val booksMalformedAttributes = "src/test/resources/books-malformed-attributes.xml"
  val datatypesValidAndInvalid = "src/test/resources/datatypes-valid-and-invalid.xml"

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
    // Fix Spark 2.0.0 on windows, see https://issues.apache.org/jira/browse/SPARK-15893
    val conf = new SparkConf().set(
      "spark.sql.warehouse.dir",
      Files.createTempDirectory("spark-warehouse").toString)
    sqlContext = new SQLContext(new SparkContext("local[2]", "XmlSuite", conf))
  }

  override protected def afterAll(): Unit = {
    try {
      sqlContext.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }

  test("DSL test nulls out invalid values when set to permissive and given explicit schema") {
    val schema = StructType(List(
      StructField("integer_value", StructType(List(
        StructField("_VALUE", IntegerType, nullable = true),
        StructField("_int", IntegerType, nullable = true)
      ))),
      StructField("long_value", StructType(List(
        StructField("_VALUE", LongType, nullable = true),
        StructField("_int", IntegerType, nullable = true)
      ))),
      StructField("float_value", FloatType, nullable = true),
      StructField("double_value", DoubleType, nullable = true),
      StructField("boolean_value", BooleanType, nullable = true),
      StructField("string_value", StringType, nullable = true),
      StructField("integer_array", ArrayType(IntegerType), nullable = true)
    ))
    val results = sqlContext.read.format("xml")
      .option("mode", "PERMISSIVE")
      .schema(schema)
      .load(datatypesValidAndInvalid)
    assert(results.schema == schema)
    val objects = results.take(2)
    assert(objects.apply(0).apply(0).asInstanceOf[Row].apply(0) == 10)
    assert(objects.apply(0).apply(0).asInstanceOf[Row].apply(1) == 10)
    assert(objects.apply(0).apply(1).asInstanceOf[Row].apply(0) == 10L)
    assert(objects.apply(0).apply(1).asInstanceOf[Row].apply(1) == null)
    assert(objects.apply(0).apply(2) == 10.0)
    assert(objects.apply(0).apply(3) == 10.0D)
    assert(objects.apply(0).apply(4) == true)
    assert(objects.apply(0).apply(5) == "Ten")
    assert(objects.apply(0).apply(6) === Array(1, 2))
    assert(objects.apply(1).apply(0).asInstanceOf[Row].apply(0) == null)
    assert(objects.apply(1).apply(0).asInstanceOf[Row].apply(1) == null)
    assert(objects.apply(1).apply(1).asInstanceOf[Row].apply(0) == null)
    assert(objects.apply(1).apply(1).asInstanceOf[Row].apply(1) == 10)
    assert(objects.apply(1).apply(2) == null)
    assert(objects.apply(1).apply(3) == null)
    assert(objects.apply(1).apply(4) == null)
    assert(objects.apply(1).apply(5) == "Ten")
    assert(objects.apply(1).apply(6) === Array(null, 2))
  }

  test("DSL test") {
    val results = sqlContext.read.format("xml")
      .load(carsFile)
      .select("year")
      .collect()

    assert(results.size === numCars)
  }

  test("DSL test with xml having unbalanced datatypes") {
    val results = sqlContext.read
      .option("treatEmptyValuesAsNulls", "true")
      .xml(gpsEmptyField)

    assert(results.collect().size === numGPS)
  }

  test("DSL test with mixed elements (attributes, no child)") {
    val results = sqlContext.read.format("xml")
      .load(carsMixedAttrNoChildFile)
      .select("date")
      .collect()

    val attrValOne = results(0).get(0).asInstanceOf[Row](1)
    val attrValTwo = results(1).get(0).asInstanceOf[Row](1)
    assert(attrValOne == "string")
    assert(attrValTwo == "struct")
    assert(results.size === numCars)
  }

  test("DSL test for inconsistent element attributes as fields") {
    val results = sqlContext.read.format("xml")
      .option("rowTag", booksTag)
      .load(booksAttributesInNoChild)
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
    val results = sqlContext.read.format("xml")
      .option("rowTag", agesTag)
      .load(agesMixedTypes)
      .collect()
    assert(results.size === numAges)
  }

  test("DSL test with elements in array having attributes") {
    val results = sqlContext.read.format("xml")
      .option("rowTag", agesTag)
      .load(agesFile)
      .collect()
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
    val results = sqlContext.read.format("xml")
      .load(carsFileGzip)
      .select("year")
      .collect()

    assert(results.size === numCars)
  }

  test("DSL test splittable compressed file") {
    val results = sqlContext.read.format("xml")
      .load(carsFileBzip2)
      .select("year")
      .collect()

    assert(results.size === numCars)
  }

  test("DSL test bad charset name") {
    val exception = intercept[UnsupportedCharsetException] {
      val results = sqlContext.read.format("xml")
        .option("charset", "1-9588-osi")
        .load(carsFile)
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
    val carsDf = new XmlReader()
      .withParseMode(ParseModes.PERMISSIVE_MODE)
      .withColumnNameOfCorruptRecord("_malformed_records")
      .xmlFile(sqlContext, carsMalformedFile)
    val cars = carsDf.collect()
    assert(cars.length == 3)

    val malformedRowOne = carsDf.select("_malformed_records").first().toSeq.head.toString
    val malformedRowTwo = carsDf.select("_malformed_records").take(2).last.toSeq.head.toString
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

    val cars = sqlContext.read.format("xml").load(carsFile)
    cars.write
      .format("xml")
      .mode(SaveMode.Overwrite)
      .options(Map("path" -> copyFilePath, "codec" -> classOf[GzipCodec].getName))
      .save(copyFilePath)
    val carsCopyPartFile = new File(copyFilePath, "part-00000.gz")
    // Check that the part file has a .gz extension
    assert(carsCopyPartFile.exists())

    val carsCopy = sqlContext.read.format("xml").load(copyFilePath)

    assert(carsCopy.count == cars.count)
    assert(carsCopy.collect.map(_.toString).toSet == cars.collect.map(_.toString).toSet)
  }

  test("DSL save with gzip compression codec by shorten name") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "cars-copy.xml"

    val cars = sqlContext.read.format("xml").load(carsFile)
    cars.write
      .format("xml")
      .mode(SaveMode.Overwrite)
      .options(Map("path" -> copyFilePath, "compression" -> "gZiP"))
      .save(copyFilePath)

    val carsCopyPartFile = new File(copyFilePath, "part-00000.gz")
    // Check that the part file has a .gz extension
    assert(carsCopyPartFile.exists())

    val carsCopy = sqlContext.read.format("xml").load(copyFilePath)

    assert(carsCopy.count == cars.count)
    assert(carsCopy.collect.map(_.toString).toSet == cars.collect.map(_.toString).toSet)
  }

  test("DSL save") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "books-copy.xml"

    val books = sqlContext.read.format("xml")
      .option("rowTag", booksTag)
      .load(booksComplicatedFile)
    books.write
      .options(Map("rootTag" -> booksRootTag, "rowTag" -> booksTag))
      .format("xml")
      .save(copyFilePath)

    val booksCopy = sqlContext.read.format("xml")
      .option("rowTag", booksTag)
      .load(copyFilePath)
    assert(booksCopy.count == books.count)
    assert(booksCopy.collect.map(_.toString).toSet === books.collect.map(_.toString).toSet)
  }

  test("DSL save with nullValue and treatEmptyValuesAsNulls") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "books-copy.xml"

    val books = sqlContext.read.format("xml")
      .option("rowTag", booksTag)
      .load(booksComplicatedFile)
    books.write
      .options(Map("rootTag" -> booksRootTag, "rowTag" -> booksTag, "nullValue" -> ""))
      .format("xml")
      .save(copyFilePath)

    val booksCopy = sqlContext.read.format("xml")
      .option("rowTag", booksTag)
      .option("treatEmptyValuesAsNulls", "true")
      .load(copyFilePath)

    assert(booksCopy.count == books.count)
    assert(booksCopy.collect.map(_.toString).toSet === books.collect.map(_.toString).toSet)
  }

  test("Write values properly as given to valueTag even if it starts with attributePrefix") {
    // Create temp directory
    TestUtils.deleteRecursively(new File(tempEmptyDir))
    new File(tempEmptyDir).mkdirs()
    val copyFilePath = tempEmptyDir + "books-copy.xml"

    val rootTag = "catalog"
    val books = sqlContext.read.format("xml")
      .option("valueTag", "#VALUE")
      .option("attributePrefix", "#")
      .option("rowTag", booksTag)
      .load(booksAttributesInNoChild)

    books.write
      .format("xml")
      .option("valueTag", "#VALUE")
      .option("attributePrefix", "#")
      .option("rootTag", rootTag)
      .option("rowTag", booksTag)
      .save(copyFilePath)

    val booksCopy = sqlContext.read.format("xml")
      .option("valueTag", "#VALUE")
      .option("attributePrefix", "_")
      .option("rowTag", booksTag)
      .load(copyFilePath)

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
    df.write.xml(copyFilePath)

    // When [[ArrayType]] has [[ArrayType]] as elements, it is confusing what is the element
    // name for XML file. Now, it is "item". So, "item" field is additionally added
    // to wrap the element.
    val schemaCopy = StructType(
      List(StructField("a", ArrayType(
        StructType(List(StructField("item", ArrayType(StringType), nullable = true)))),
          nullable = true)))
    val dfCopy = sqlContext.read.format("xml").load(copyFilePath)

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
    df.write.format("xml").save(copyFilePath)

    val dfCopy = new XmlReader()
      .withSchema(schema)
      .xmlFile(sqlContext, copyFilePath)

    assert(dfCopy.collect() === df.collect())
    assert(dfCopy.schema === df.schema)
  }

  test("DSL test schema inferred correctly") {
    val results = sqlContext.read.format("xml").option("rowTag", booksTag).load(booksFile)

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
    val results = sqlContext.read.format("xml")
      .option("rowTag", booksTag)
      .option("samplingRatio", 0.5)
      .load(booksFile)

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
    val results = sqlContext.read.format("xml")
      .option("rowTag", booksTag)
      .load(booksNestedObjectFile)

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
    val results = sqlContext.read.format("xml")
      .option("rowTag", booksTag)
      .load(booksNestedArrayFile)

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
    val results = sqlContext.read.format("xml")
      .option("rowTag", booksTag)
      .load(booksComplicatedFile)

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
    val dataFrame = sqlContext.read.format("xml").load(carsFile)

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
    val results = sqlContext.read.format("xml")
      .option("rowTag", topicsTag)
      .load(topicsFile)
      .collect()

    assert(results.size === numTopics)
  }

  test("Missing nested struct represented as null instead of empty Row") {
    val result = sqlContext.read.format("xml")
      .option("rowTag", "item")
      .load(nullNestedStructFile)
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
    assert(df.schema == schema)
  }

  test("Skip and project currecntly XML files without indentation") {
    val df = sqlContext.read.format("xml").load(carsNoIndentationFile)
    val results = df.select("model").collect()
    val years = results.map(_.toSeq.head).toSet
    assert(years == Set("S", "E350", "Volt"))
  }

  test("Select correctly all child fields regardless of pushed down projection") {
    val results = sqlContext.read.format("xml")
      .option("rowTag", "book")
      .load(booksComplicatedFile)
      .selectExpr("publish_dates")
      .collect()
    results.foreach { row =>
      // All nested fields should not have nulls but arrays.
      assert(!row.anyNull)
    }
  }

  test("Empty string not allowed for rowTag, attributePrefix and valueTag.") {
    val messageOne = intercept[IllegalArgumentException] {
      sqlContext.read.format("xml").option("rowTag", "").load(carsFile)
    }.getMessage
    assert(messageOne == "requirement failed: 'rowTag' option should not be empty string.")

    val messageTwo = intercept[IllegalArgumentException] {
      sqlContext.read.format("xml").option("attributePrefix", "").load(carsFile)
    }.getMessage
    assert(
      messageTwo == "requirement failed: 'attributePrefix' option should not be empty string.")

    val messageThree = intercept[IllegalArgumentException] {
      sqlContext.read.format("xml").option("valueTag", "").load(carsFile)
    }.getMessage
    assert(messageThree == "requirement failed: 'valueTag' option should not be empty string.")
  }

  test("valueTag and attributePrefix should not be the same.") {
    val messageOne = intercept[IllegalArgumentException] {
      sqlContext.read.format("xml")
        .option("valueTag", "#abc")
        .option("attributePrefix", "#abc")
        .load(carsFile)
    }.getMessage
    assert(messageOne ==
      "requirement failed: 'valueTag' and 'attributePrefix' options should not be the same.")
  }

  test("nullValue and treatEmptyValuesAsNulls test") {
    val resultsOne = sqlContext.read.format("xml")
      .option("treatEmptyValuesAsNulls", "true")
      .load(gpsEmptyField)
    assert(resultsOne.selectExpr("extensions.TrackPointExtension").head().isNullAt(0))
    assert(resultsOne.collect().size === numGPS)

    val resultsTwo = sqlContext.read.format("xml")
      .option("nullValue", "2013-01-24T06:18:43Z")
      .load(gpsEmptyField)
    assert(resultsTwo.selectExpr("time").head().isNullAt(0))
    assert(resultsTwo.collect().size === numGPS)
  }

  test("ignoreSurroundingSpaces test") {
    val results = new XmlReader()
      .withIgnoreSurroundingSpaces(true)
      .withRowTag(agesTag)
      .xmlFile(sqlContext, agesWithSpacesFile)
      .collect()
    val attrValOne = results(0).get(0).asInstanceOf[Row](1)
    val attrValTwo = results(1).get(0).asInstanceOf[Row](0)
    assert(attrValOne === "1990-02-24")
    assert(attrValTwo === 30)
    assert(results.length === numAges)
  }

  test("DSL test with malformed attributes") {
    val results = new XmlReader()
      .withParseMode(ParseModes.DROP_MALFORMED_MODE)
        .withRowTag(booksTag)
        .xmlFile(sqlContext, booksMalformedAttributes)
        .collect()

    assert(results.length === 2)
    assert(results(0)(0) === "bk111")
    assert(results(1)(0) === "bk112")
  }
}
