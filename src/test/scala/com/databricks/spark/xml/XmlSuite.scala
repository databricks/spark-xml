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

import java.nio.charset.{StandardCharsets, UnsupportedCharsetException}
import java.nio.file.{Files, Path}
import java.sql.{Date, Timestamp}

import scala.io.Source

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.io.compress.GzipCodec
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import com.databricks.spark.xml.XmlOptions._
import com.databricks.spark.xml.util._

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.SparkException

final class XmlSuite extends FunSuite with BeforeAndAfterAll {

  private val resDir = "src/test/resources/"
  private val agesFile = resDir + "ages.xml"
  private val agesWithSpacesFile = resDir + "ages-with-spaces.xml"
  private val booksFile = resDir + "books.xml"
  private val booksNestedObjectFile = resDir + "books-nested-object.xml"
  private val booksNestedArrayFile = resDir + "books-nested-array.xml"
  private val booksComplicatedFile = resDir + "books-complicated.xml"
  private val booksComplicatedFileNullAttribute =
    resDir + "books-complicated-null-attribute.xml"
  private val carsFile = resDir + "cars.xml"
  private val carsFile8859 = resDir + "cars-iso-8859-1.xml"
  private val carsFileGzip = resDir + "cars.xml.gz"
  private val carsFileBzip2 = resDir + "cars.xml.bz2"
  private val carsNoIndentationFile = resDir + "cars-no-indentation.xml"
  private val carsMixedAttrNoChildFile = resDir + "cars-mixed-attr-no-child.xml"
  private val booksAttributesInNoChild = resDir + "books-attributes-in-no-child.xml"
  private val carsUnbalancedFile = resDir + "cars-unbalanced-elements.xml"
  private val carsMalformedFile = resDir + "cars-malformed.xml"
  private val dataTypesValidAndInvalid = resDir + "datatypes-valid-and-invalid.xml"
  private val nullNumbersFile = resDir + "null-numbers.xml"
  private val nullEmptyStringFile = resDir + "null-empty-string.xml"
  private val emptyFile = resDir + "empty.xml"
  private val topicsFile = resDir + "topics-namespaces.xml"
  private val gpsEmptyField = resDir + "gps-empty-field.xml"
  private val agesMixedTypes = resDir + "ages-mixed-types.xml"
  private val nullNestedStructFile = resDir + "null-nested-struct.xml"
  private val nullNestedStructFile2 = resDir + "null-nested-struct-2.xml"
  private val simpleNestedObjects = resDir + "simple-nested-objects.xml"
  private val nestedElementWithNameOfParent =
    resDir + "nested-element-with-name-of-parent.xml"
  private val nestedElementWithAttributesAndNameOfParent =
    resDir + "nested-element-with-attributes-and-name-of-parent.xml"
  private val booksMalformedAttributes = resDir + "books-malformed-attributes.xml"
  private val fiasHouse = resDir + "fias_house.xml"
  private val attributesStartWithNewLine = resDir + "attributesStartWithNewLine.xml"
  private val attributesStartWithNewLineLF = resDir + "attributesStartWithNewLineLF.xml"
  private val attributesStartWithNewLineCR = resDir + "attributesStartWithNewLineCR.xml"
  private val selfClosingTag = resDir + "self-closing-tag.xml"
  private val textColumn = resDir + "textColumn.xml"
  private val processing = resDir + "processing.xml"
  private val mixedChildren = resDir + "mixed_children.xml"
  private val mixedChildren2 = resDir + "mixed_children_2.xml"

  private val booksTag = "book"
  private val booksRootTag = "books"
  private val topicsTag = "Topic"
  private val agesTag = "person"
  private val fiasRowTag = "House"

  private val numAges = 3
  private val numCars = 3
  private val numBooks = 12
  private val numBooksComplicated = 3
  private val numTopics = 1
  private val numGPS = 2
  private val numFiasHouses = 37

  private var spark: SparkSession = _
  private var tempDir: Path = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder().
      master("local[2]").
      appName("XmlSuite").
      config("spark.ui.enabled", false).
      getOrCreate()
    tempDir = Files.createTempDirectory("XmlSuite")
    tempDir.toFile.deleteOnExit()
  }

  override protected def afterAll(): Unit = {
    try {
      spark.stop()
      spark = null
    } finally {
      super.afterAll()
    }
  }

  private def getEmptyTempDir(): Path = {
    Files.createTempDirectory(tempDir, "test")
  }

  // Utilities to simplify schema specification:

  private def buildSchema(fields: StructField*): StructType =
    StructType(fields)

  private def field(
      name: String,
      dataType: DataType = StringType,
      nullable: Boolean = true): StructField =
    StructField(name, dataType, nullable)

  private def struct(fields: StructField*): StructType =
    buildSchema(fields: _*)

  private def struct(name: String, fields: StructField*): StructField =
    field(name, struct(fields: _*))

  private def structArray(name: String, fields: StructField*): StructField =
    field(name, ArrayType(struct(fields: _*)))

  private def array(name: String, dataType: DataType): StructField =
    field(name, ArrayType(dataType))

  // Tests

  test("DSL test") {
    val results = spark.read.format("xml")
      .load(carsFile)
      .select("year")
      .collect()

    assert(results.length === numCars)
  }

  test("DSL test with xml having unbalanced datatypes") {
    val results = spark.read
      .option("treatEmptyValuesAsNulls", "true")
      .xml(gpsEmptyField)

    assert(results.collect().length === numGPS)
  }

  test("DSL test with mixed elements (attributes, no child)") {
    val results = spark.read
      .xml(carsMixedAttrNoChildFile)
      .select("date")
      .collect()

    val attrValOne = results(0).getStruct(0)(1)
    val attrValTwo = results(1).getStruct(0)(1)
    assert(attrValOne == "string")
    assert(attrValTwo == "struct")
    assert(results.length === numCars)
  }

  test("DSL test for inconsistent element attributes as fields") {
    val results = spark.read
      .option("rowTag", booksTag)
      .xml(booksAttributesInNoChild)
      .select("price")

    // This should not throw an exception `java.lang.ArrayIndexOutOfBoundsException`
    // as non-existing values are represented as `null`s.
    assert(results.collect()(0).getStruct(0).get(1) === null)
  }

  test("DSL test with mixed elements (struct, string)") {
    val results = spark.read
      .option("rowTag", agesTag)
      .xml(agesMixedTypes)
      .collect()
    assert(results.length === numAges)
  }

  test("DSL test with elements in array having attributes") {
    val results = spark.read
      .option("rowTag", agesTag)
      .xml(agesFile)
      .collect()
    val attrValOne = results(0).getStruct(0)(1)
    val attrValTwo = results(1).getStruct(0)(1)
    assert(attrValOne == "1990-02-24")
    assert(attrValTwo == "1985-01-01")
    assert(results.length === numAges)
  }

  test("DSL test for iso-8859-1 encoded file") {
    val dataFrame = new XmlReader()
      .withCharset(StandardCharsets.ISO_8859_1.name)
      .xmlFile(spark, carsFile8859)
    assert(dataFrame.select("year").collect().length === numCars)

    val results = dataFrame
      .select("comment", "year")
      .where(dataFrame("year") === 2012)

    assert(results.head() === Row("No comment", 2012))
  }

  test("DSL test compressed file") {
    val results = spark.read
      .xml(carsFileGzip)
      .select("year")
      .collect()

    assert(results.length === numCars)
  }

  test("DSL test splittable compressed file") {
    val results = spark.read
      .xml(carsFileBzip2)
      .select("year")
      .collect()

    assert(results.length === numCars)
  }

  test("DSL test bad charset name") {
    val exception = intercept[UnsupportedCharsetException] {
      spark.read
        .option("charset", "1-9588-osi")
        .xml(carsFile)
        .select("year")
        .collect()
    }
    assert(exception.getMessage.contains("1-9588-osi"))
  }

  test("DDL test") {
    spark.sql(
      s"""
         |CREATE TEMPORARY VIEW carsTable1
         |USING com.databricks.spark.xml
         |OPTIONS (path "$carsFile")
      """.stripMargin.replaceAll("\n", " "))

    assert(spark.sql("SELECT year FROM carsTable1").collect().length === numCars)
  }

  test("DDL test with alias name") {
    spark.sql(
      s"""
         |CREATE TEMPORARY VIEW carsTable2
         |USING xml
         |OPTIONS (path "$carsFile")
      """.stripMargin.replaceAll("\n", " "))

    assert(spark.sql("SELECT year FROM carsTable2").collect().length === numCars)
  }

  test("DSL test for parsing a malformed XML file") {
    val results = new XmlReader()
      .withParseMode(DropMalformedMode.name)
      .xmlFile(spark, carsMalformedFile)

    assert(results.count() === 1)
  }

  test("DSL test for dropping malformed rows") {
    val cars = new XmlReader()
      .withParseMode(DropMalformedMode.name)
      .xmlFile(spark, carsMalformedFile)

    assert(cars.count() == 1)
    assert(cars.head() === Row("Chevy", "Volt", 2015))
  }

  test("DSL test for failing fast") {
    val exceptionInParse = intercept[SparkException] {
      new XmlReader()
        .withFailFast(true)
        .xmlFile(spark, carsMalformedFile)
        .collect()
    }
    assert(exceptionInParse.getMessage.contains("Malformed line in FAILFAST mode"))
  }

  test("DSL test for permissive mode for corrupt records") {
    val carsDf = new XmlReader()
      .withParseMode(PermissiveMode.name)
      .withColumnNameOfCorruptRecord("_malformed_records")
      .xmlFile(spark, carsMalformedFile)
    val cars = carsDf.collect()
    assert(cars.length === 3)

    val malformedRowOne = carsDf.select("_malformed_records").first().get(0).toString
    val malformedRowTwo = carsDf.select("_malformed_records").take(2).last.get(0).toString
    val expectedMalformedRowOne = "<ROW><year>2012</year><make>Tesla</make><model>>S" +
      "<comment>No comment</comment></ROW>"
    val expectedMalformedRowTwo = "<ROW></year><make>Ford</make><model>E350</model>model></model>" +
      "<comment>Go get one now they are going fast</comment></ROW>"

    assert(malformedRowOne.replaceAll("\\s", "") === expectedMalformedRowOne.replaceAll("\\s", ""))
    assert(malformedRowTwo.replaceAll("\\s", "") === expectedMalformedRowTwo.replaceAll("\\s", ""))
    assert(cars(2)(0) === null)
    assert(cars(0).toSeq.takeRight(3) === Seq(null, null, null))
    assert(cars(1).toSeq.takeRight(3) === Seq(null, null, null))
    assert(cars(2).toSeq.takeRight(3) === Seq("Chevy", "Volt", 2015))
  }

  test("DSL test with empty file and known schema") {
    val results = new XmlReader()
      .withSchema(buildSchema(field("column", StringType, false)))
      .xmlFile(spark, emptyFile)
      .count()

    assert(results === 0)
  }

  test("DSL test with poorly formatted file and string schema") {
    val schema = buildSchema(
      field("color"),
      field("year"),
      field("make"),
      field("model"),
      field("comment"))
    val results = new XmlReader()
      .withSchema(schema)
      .xmlFile(spark, carsUnbalancedFile)
      .count()

    assert(results === numCars)
  }

  test("DDL test with empty file") {
    spark.sql(s"""
           |CREATE TEMPORARY VIEW carsTable3
           |(year double, make string, model string, comments string, grp string)
           |USING com.databricks.spark.xml
           |OPTIONS (path "$emptyFile")
      """.stripMargin.replaceAll("\n", " "))

    assert(spark.sql("SELECT count(*) FROM carsTable3").collect().head(0) === 0)
  }

  test("SQL test insert overwrite") {
    val tempPath = getEmptyTempDir()
    spark.sql(
      s"""
         |CREATE TEMPORARY VIEW booksTableIO
         |USING com.databricks.spark.xml
         |OPTIONS (path "$booksFile", rowTag "$booksTag")
      """.stripMargin.replaceAll("\n", " "))
    spark.sql(
      s"""
         |CREATE TEMPORARY VIEW booksTableEmpty
         |(author string, description string, genre string,
         |id string, price double, publish_date string, title string)
         |USING com.databricks.spark.xml
         |OPTIONS (path "$tempPath")
      """.stripMargin.replaceAll("\n", " "))

    assert(spark.sql("SELECT * FROM booksTableIO").collect().length === numBooks)
    assert(spark.sql("SELECT * FROM booksTableEmpty").collect().isEmpty)

    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE booksTableEmpty
         |SELECT * FROM booksTableIO
      """.stripMargin.replaceAll("\n", " "))
    assert(spark.sql("SELECT * FROM booksTableEmpty").collect().length == numBooks)
  }

  test("DSL save with gzip compression codec") {
    val copyFilePath = getEmptyTempDir().resolve("cars-copy.xml")

    val cars = spark.read.xml(carsFile)
    cars.write
      .mode(SaveMode.Overwrite)
      .options(Map("codec" -> classOf[GzipCodec].getName))
      .xml(copyFilePath.toString)
    // Check that the part file has a .gz extension
    assert(Files.exists(copyFilePath.resolve("part-00000.gz")))

    val carsCopy = spark.read.xml(copyFilePath.toString)

    assert(carsCopy.count === cars.count)
    assert(carsCopy.collect.map(_.toString).toSet === cars.collect.map(_.toString).toSet)
  }

  test("DSL save with gzip compression codec by shorten name") {
    val copyFilePath = getEmptyTempDir().resolve("cars-copy.xml")

    val cars = spark.read.xml(carsFile)
    cars.write
      .mode(SaveMode.Overwrite)
      .options(Map("compression" -> "gZiP"))
      .xml(copyFilePath.toString)

    // Check that the part file has a .gz extension
    assert(Files.exists(copyFilePath.resolve("part-00000.gz")))

    val carsCopy = spark.read.xml(copyFilePath.toString)

    assert(carsCopy.count === cars.count)
    assert(carsCopy.collect.map(_.toString).toSet === cars.collect.map(_.toString).toSet)
  }

  test("DSL save") {
    val copyFilePath = getEmptyTempDir().resolve("books-copy.xml")

    val books = spark.read
      .option("rowTag", booksTag)
      .xml(booksComplicatedFile)
    books.write
      .options(Map("rootTag" -> booksRootTag, "rowTag" -> booksTag))
      .xml(copyFilePath.toString)

    val booksCopy = spark.read
      .option("rowTag", booksTag)
      .xml(copyFilePath.toString)
    assert(booksCopy.count === books.count)
    assert(booksCopy.collect.map(_.toString).toSet === books.collect.map(_.toString).toSet)
  }

  test("DSL save with nullValue and treatEmptyValuesAsNulls") {
    val copyFilePath = getEmptyTempDir().resolve("books-copy.xml")

    val books = spark.read
      .option("rowTag", booksTag)
      .xml(booksComplicatedFile)
    books.write
      .options(Map("rootTag" -> booksRootTag, "rowTag" -> booksTag, "nullValue" -> ""))
      .xml(copyFilePath.toString)

    val booksCopy = spark.read
      .option("rowTag", booksTag)
      .option("treatEmptyValuesAsNulls", "true")
      .xml(copyFilePath.toString)

    assert(booksCopy.count === books.count)
    assert(booksCopy.collect.map(_.toString).toSet === books.collect.map(_.toString).toSet)
  }

  test("Write values properly as given to valueTag even if it starts with attributePrefix") {
    val copyFilePath = getEmptyTempDir().resolve("books-copy.xml")

    val rootTag = "catalog"
    val books = spark.read
      .option("valueTag", "#VALUE")
      .option("attributePrefix", "#")
      .option("rowTag", booksTag)
      .xml(booksAttributesInNoChild)

    books.write
      .option("valueTag", "#VALUE")
      .option("attributePrefix", "#")
      .option("rootTag", rootTag)
      .option("rowTag", booksTag)
      .xml(copyFilePath.toString)

    val booksCopy = spark.read
      .option("valueTag", "#VALUE")
      .option("attributePrefix", "_")
      .option("rowTag", booksTag)
      .xml(copyFilePath.toString)

    assert(booksCopy.count === books.count)
    assert(booksCopy.collect.map(_.toString).toSet === books.collect.map(_.toString).toSet)
  }

  test("DSL save dataframe not read from a XML file") {
    val copyFilePath = getEmptyTempDir().resolve("data-copy.xml")

    val schema = buildSchema(array("a", ArrayType(StringType)))
    val data = spark.sparkContext.parallelize(
      List(List(List("aa", "bb"), List("aa", "bb")))).map(Row(_))
    val df = spark.createDataFrame(data, schema)
    df.write.xml(copyFilePath.toString)

    // When [[ArrayType]] has [[ArrayType]] as elements, it is confusing what is the element
    // name for XML file. Now, it is "item". So, "item" field is additionally added
    // to wrap the element.
    val schemaCopy = buildSchema(
      structArray("a",
        field("item", ArrayType(StringType))))
    val dfCopy = spark.read.xml(copyFilePath.toString)

    assert(dfCopy.count === df.count)
    assert(dfCopy.schema === schemaCopy)
  }

  test("DSL save dataframe with data types correctly") {
    val copyFilePath = getEmptyTempDir().resolve("data-copy.xml")

    // Create the schema.
    val dataTypes = Array(
        StringType, NullType, BooleanType,
        ByteType, ShortType, IntegerType, LongType,
        FloatType, DoubleType, DecimalType(25, 3), DecimalType(6, 5),
        DateType, TimestampType, MapType(StringType, StringType))
    val fields = dataTypes.zipWithIndex.map { case (dataType, index) =>
      field(s"col$index", dataType)
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
    val data = spark.sparkContext.parallelize(Seq(row))

    val df = spark.createDataFrame(data, schema)
    df.write.xml(copyFilePath.toString)

    val dfCopy = new XmlReader()
      .withSchema(schema)
      .xmlFile(spark, copyFilePath.toString)

    assert(dfCopy.collect() === df.collect())
    assert(dfCopy.schema === df.schema)
  }

  test("DSL test schema inferred correctly") {
    val results = spark.read.option("rowTag", booksTag).xml(booksFile)

    assert(results.schema === buildSchema(
      field(s"${DEFAULT_ATTRIBUTE_PREFIX}id"),
      field("author"),
      field("description"),
      field("genre"),
      field("price", DoubleType),
      field("publish_date"),
      field("title")))

    assert(results.collect().length === numBooks)
  }

  test("DSL test schema inferred correctly with sampling ratio") {
    val results = spark.read
      .option("rowTag", booksTag)
      .option("samplingRatio", 0.5)
      .xml(booksFile)

    assert(results.schema === buildSchema(
      field(s"${DEFAULT_ATTRIBUTE_PREFIX}id"),
      field("author"),
      field("description"),
      field("genre"),
      field("price", DoubleType),
      field("publish_date"),
      field("title")))

    assert(results.collect().length === numBooks)
  }

  test("DSL test schema (object) inferred correctly") {
    val results = spark.read
      .option("rowTag", booksTag)
      .xml(booksNestedObjectFile)

    assert(results.schema === buildSchema(
      field(s"${DEFAULT_ATTRIBUTE_PREFIX}id"),
      field("author"),
      field("description"),
      field("genre"),
      field("price", DoubleType),
      struct("publish_dates",
        field("publish_date")),
      field("title")))

    assert(results.collect().length === numBooks)
  }

  test("DSL test schema (array) inferred correctly") {
    val results = spark.read
      .option("rowTag", booksTag)
      .xml(booksNestedArrayFile)

    assert(results.schema === buildSchema(
      field(s"${DEFAULT_ATTRIBUTE_PREFIX}id"),
      field("author"),
      field("description"),
      field("genre"),
      field("price", DoubleType),
      array("publish_date", StringType),
      field("title")))

    assert(results.collect().length === numBooks)
  }

  test("DSL test schema (complicated) inferred correctly") {
    val results = spark.read
      .option("rowTag", booksTag)
      .xml(booksComplicatedFile)

    assert(results.schema == buildSchema(
      field(s"${DEFAULT_ATTRIBUTE_PREFIX}id"),
      field("author"),
      struct("genre",
        field("genreid", LongType),
        field("name")),
      field("price", DoubleType),
      struct("publish_dates",
        array("publish_date",
          struct(
            field(s"${DEFAULT_ATTRIBUTE_PREFIX}tag"),
            field("day", LongType),
            field("month", LongType),
            field("year", LongType)))),
      field("title")))

    assert(results.collect().length === numBooksComplicated)
  }

  test("DSL test parsing and inferring attribute in elements having no child element") {
    // Default value.
    val resultsOne = new XmlReader()
      .withRowTag(booksTag)
      .xmlFile(spark, booksAttributesInNoChild)

    val schemaOne = buildSchema(
      field("_id"),
      field("author"),
      struct("price",
        field("_VALUE"),
        field(s"_unit")),
      field("publish_date"),
      field("title"))

    assert(resultsOne.schema === schemaOne)
    assert(resultsOne.count === numBooks)

    // Explicitly set
    val attributePrefix = "@#"
    val valueTag = "#@@value"
    val resultsTwo = new XmlReader()
      .withRowTag(booksTag)
      .withAttributePrefix(attributePrefix)
      .withValueTag(valueTag)
      .xmlFile(spark, booksAttributesInNoChild)

    val schemaTwo = buildSchema(
      field(s"${attributePrefix}id"),
      field("author"),
      struct("price",
        field(valueTag),
        field(s"${attributePrefix}unit")),
      field("publish_date"),
      field("title"))

    assert(resultsTwo.schema === schemaTwo)
    assert(resultsTwo.count === numBooks)
  }

  test("DSL test schema (excluding tags) inferred correctly") {
    val results = new XmlReader()
      .withExcludeAttribute(true)
      .withRowTag(booksTag)
      .xmlFile(spark, booksFile)

    val schema = buildSchema(
      field("author"),
      field("description"),
      field("genre"),
      field("price", DoubleType),
      field("publish_date"),
      field("title"))

    assert(results.schema === schema)
  }

  test("DSL test with custom schema") {
    val schema = buildSchema(
      field("make"),
      field("model"),
      field("comment"),
      field("color"),
      field("year", IntegerType))
    val results = new XmlReader()
      .withSchema(schema)
      .xmlFile(spark, carsUnbalancedFile)
      .count()

    assert(results === numCars)
  }

  test("DSL test inferred schema passed through") {
    val dataFrame = spark.read.xml(carsFile)

    val results = dataFrame
      .select("comment", "year")
      .where(dataFrame("year") === 2012)

    assert(results.head() === Row("No comment", 2012))
  }

  test("DSL test nullable fields") {
    val schema = buildSchema(
      field("name", StringType, false),
      field("age"))
    val results = new XmlReader()
      .withSchema(schema)
      .xmlFile(spark, nullNumbersFile)
      .collect()

    assert(results(0) === Row("alice", "35"))
    assert(results(1) === Row("bob", "    "))
    assert(results(2) === Row("coc", "24"))
  }

  test("DSL test for treating empty string as null value") {
    val schema = buildSchema(
      field("name", StringType, false),
      field("age", IntegerType))
    val results = new XmlReader()
      .withSchema(schema)
      .withTreatEmptyValuesAsNulls(true)
      .xmlFile(spark, nullNumbersFile)
      .collect()

    assert(results(1) === Row("bob", null))
  }

  test("DSL test with namespaces ignored") {
    val results = spark.read
      .option("rowTag", topicsTag)
      .xml(topicsFile)
      .collect()

    assert(results.length === numTopics)
  }

  test("Missing nested struct represented as Row of nulls instead of null") {
    val result = spark.read
      .option("rowTag", "item")
      .xml(nullNestedStructFile)
      .select("b.es")
      .collect()

    assert(result(1).getStruct(0) !== null)
    assert(result(1).getStruct(0)(0) === null)
  }

  test("Produces correct result for empty vs non-existent rows") {
    val schema = buildSchema(
      struct("b",
        struct("es",
          field("e"),
          field("f"))))
    val result = spark.read
      .option("rowTag", "item")
      .schema(schema)
      .xml(nullNestedStructFile2)
      .collect()

    assert(result(0) === Row(Row(null)))
    assert(result(1) === Row(Row(Row(null, null))))
    assert(result(2) === Row(Row(Row("E", null))))
    assert(result(3) === Row(Row(Row("E", " "))))
    assert(result(4) === Row(Row(Row("E", ""))))
  }

  test("Produces correct order of columns for nested rows when user specifies a schema") {
    val schema = buildSchema(
      struct("c",
        field("b", IntegerType),
        field("a", IntegerType)))

    val result = new XmlReader()
      .withSchema(schema)
      .xmlFile(spark, simpleNestedObjects)
      .select("c.a", "c.b")
      .collect()

    assert(result(0) === Row(111, 222))
  }

  private[this] def testNextedElementFromFile(xmlFile: String) = {
    val lines = Source.fromFile(xmlFile).getLines.toList
    val firstExpected = lines(2).trim
    val lastExpected = lines(3).trim
    val config = new Configuration(spark.sparkContext.hadoopConfiguration)
    config.set(XmlInputFormat.START_TAG_KEY, "<parent>")
    config.set(XmlInputFormat.END_TAG_KEY, "</parent>")
    val records = spark.sparkContext.newAPIHadoopFile(
      xmlFile,
      classOf[XmlInputFormat],
      classOf[LongWritable],
      classOf[Text],
      config)
    val list = records.values.map(_.toString).collect().toList
    assert(list.length === 2)
    val firstActual = list.head
    val lastActual = list.last
    assert(firstActual === firstExpected)
    assert(lastActual === lastExpected)
  }

  test("Nested element with same name as parent delineation") {
    testNextedElementFromFile(nestedElementWithNameOfParent)
  }

  test("Nested element including attribute with same name as parent delineation") {
    testNextedElementFromFile(nestedElementWithAttributesAndNameOfParent)
  }

  test("Nested element with same name as parent schema inference") {
    val df = new XmlReader()
      .withRowTag("parent")
      .xmlFile(spark, nestedElementWithNameOfParent)

    val schema = buildSchema(
      field("child"),
      struct("parent",
        field("child")))
    assert(df.schema === schema)
  }

  test("Skip and project currently XML files without indentation") {
    val df = spark.read.xml(carsNoIndentationFile)
    val results = df.select("model").collect()
    val years = results.map(_(0)).toSet
    assert(years === Set("S", "E350", "Volt"))
  }

  test("Select correctly all child fields regardless of pushed down projection") {
    val results = spark.read
      .option("rowTag", "book")
      .xml(booksComplicatedFile)
      .selectExpr("publish_dates")
      .collect()
    results.foreach { row =>
      // All nested fields should not have nulls but arrays.
      assert(!row.anyNull)
    }
  }

  test("Empty string not allowed for rowTag, attributePrefix and valueTag.") {
    val messageOne = intercept[IllegalArgumentException] {
      spark.read.option("rowTag", "").xml(carsFile)
    }.getMessage
    assert(messageOne === "requirement failed: 'rowTag' option should not be empty string.")

    val messageTwo = intercept[IllegalArgumentException] {
      spark.read.option("attributePrefix", "").xml(carsFile)
    }.getMessage
    assert(
      messageTwo === "requirement failed: 'attributePrefix' option should not be empty string.")

    val messageThree = intercept[IllegalArgumentException] {
      spark.read.option("valueTag", "").xml(carsFile)
    }.getMessage
    assert(messageThree === "requirement failed: 'valueTag' option should not be empty string.")
  }

  test("valueTag and attributePrefix should not be the same.") {
    val messageOne = intercept[IllegalArgumentException] {
      spark.read
        .option("valueTag", "#abc")
        .option("attributePrefix", "#abc")
        .xml(carsFile)
    }.getMessage
    assert(messageOne ===
      "requirement failed: 'valueTag' and 'attributePrefix' options should not be the same.")
  }

  test("nullValue and treatEmptyValuesAsNulls test") {
    val resultsOne = spark.read
      .option("treatEmptyValuesAsNulls", "true")
      .xml(gpsEmptyField)
    assert(resultsOne.selectExpr("extensions.TrackPointExtension").head.getStruct(0) !== null)
    assert(resultsOne.selectExpr("extensions.TrackPointExtension")
      .head.getStruct(0)(0) === null)
    // Is the behavior below consistent? see line above.
    assert(resultsOne.selectExpr("extensions.TrackPointExtension.hr").head.getStruct(0) === null)
    assert(resultsOne.collect().length === numGPS)

    val resultsTwo = spark.read
      .option("nullValue", "2013-01-24T06:18:43Z")
      .xml(gpsEmptyField)
    assert(resultsTwo.selectExpr("time").head.getStruct(0) === null)
    assert(resultsTwo.collect().length === numGPS)
  }

  test("ignoreSurroundingSpaces test") {
    val results = new XmlReader()
      .withIgnoreSurroundingSpaces(true)
      .withRowTag(agesTag)
      .xmlFile(spark, agesWithSpacesFile)
      .collect()
    val attrValOne = results(0).getStruct(0)(1)
    val attrValTwo = results(1).getStruct(0)(0)
    assert(attrValOne === "1990-02-24")
    assert(attrValTwo === 30)
    assert(results.length === numAges)
  }

  test("DSL test with malformed attributes") {
    val results = new XmlReader()
      .withParseMode(DropMalformedMode.name)
        .withRowTag(booksTag)
        .xmlFile(spark, booksMalformedAttributes)
        .collect()

    assert(results.length === 2)
    assert(results(0)(0) === "bk111")
    assert(results(1)(0) === "bk112")
  }

  test("read utf-8 encoded file with empty tag") {
    val df = spark.read
      .option("excludeAttribute", "false")
      .option("rowTag", fiasRowTag)
      .xml(fiasHouse)

    assert(df.collect().length === numFiasHouses)
    assert(df.select().where("_HOUSEID is null").count() == 0)
  }

  test("attributes start with new line") {
    val schema = buildSchema(
      field("_schemaLocation"),
      field("_xmlns"),
      field("_xsi"),
      field("body"),
      field("from"),
      field("heading"),
      field("to"))

    val rowsCount = 1

    Seq(attributesStartWithNewLine,
        attributesStartWithNewLineCR,
        attributesStartWithNewLineLF).foreach { file =>
      val df = spark.read
        .option("excludeAttribute", "false")
        .option("rowTag", "note")
        .xml(file)
      assert(df.schema === schema)
      assert(df.count() === rowsCount)
    }
  }

  test("Produces correct result for a row with a self closing tag inside") {
    val schema = buildSchema(
      field("non-empty-tag", IntegerType),
      field("self-closing-tag", IntegerType))

    val result = new XmlReader()
      .withSchema(schema)
      .xmlFile(spark, selfClosingTag)
      .collect()

    assert(result(0) === Row(1, null))
  }

  test("DSL save with null attributes") {
    val copyFilePath = getEmptyTempDir().resolve("books-copy.xml")

    val books = spark.read
      .option("rowTag", booksTag)
      .xml(booksComplicatedFileNullAttribute)
    books.write
      .options(Map("rootTag" -> booksRootTag, "rowTag" -> booksTag))
      .xml(copyFilePath.toString)

    val booksCopy = spark.read
      .option("rowTag", booksTag)
      .xml(copyFilePath.toString)
    assert(booksCopy.count === books.count)
    assert(booksCopy.collect.map(_.toString).toSet === books.collect.map(_.toString).toSet)
  }

  test("DSL test nulls out invalid values when set to permissive and given explicit schema") {
    val schema = buildSchema(
      struct("integer_value",
        field("_VALUE", IntegerType),
        field("_int", IntegerType)),
      struct("long_value",
        field("_VALUE", LongType),
        field("_int", StringType)),
      field("float_value", FloatType),
      field("double_value", DoubleType),
      field("boolean_value", BooleanType),
      field("string_value"), array("integer_array", IntegerType),
      field("integer_map", MapType(StringType, IntegerType)),
      field("_malformed_records", StringType))
    val results = spark.read
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_malformed_records")
      .schema(schema)
      .xml(dataTypesValidAndInvalid)

    assert(results.schema === schema)

    val Array(valid, invalid) = results.take(2)

    assert(valid.toSeq.toArray.take(schema.length - 1) ===
      Array(Row(10, 10), Row(10, "Ten"), 10.0, 10.0, true,
        "Ten", Array(1, 2), Map("a" -> 123, "b" -> 345)))
    assert(invalid.toSeq.toArray.take(schema.length - 1) ===
      Array(null, null, null, null, null,
        "Ten", Array(2), null))

    assert(valid.toSeq.toArray.last === null)
    assert(invalid.toSeq.toArray.last.toString.contains(
      <integer_value int="Ten">Ten</integer_value>.toString))
  }

  test("empty string to null and back") {
    val fruit = spark.read
      .option("rowTag", "row")
      .option("nullValue", "")
      .xml(nullEmptyStringFile)
    assert(fruit.head().getAs[String]("color") === null)
  }

  test("test all string data type infer strategy") {
    val text = spark.read
      .option("rowTag", "ROW")
      .option("inferSchema", "false")
      .xml(textColumn)
    assert(text.head().getAs[String]("col1") === "00010")

  }

  test("test default data type infer strategy") {
    val default = spark.read
      .option("rowTag", "ROW")
      .option("inferSchema", "true")
      .xml(textColumn)
    assert(default.head().getAs[Int]("col1") === 10)
  }

  test("test XML with processing instruction") {
    val processingDF = spark.read
      .option("rowTag", "foo")
      .option("inferSchema", "true")
      .xml(processing)
    assert(processingDF.count() === 1)
  }

  test("test mixed text and element children") {
    val mixedDF = spark.read
      .option("rowTag", "root")
      .option("inferSchema", true)
      .xml(mixedChildren)
    val mixedRow = mixedDF.head()
    assert(mixedRow.getAs[Row](0).toSeq === Seq(" lorem "))
    assert(mixedRow.getString(1) === " ipsum ")
  }

  test("test mixed text and complex element children") {
    val mixedDF = spark.read
      .option("rowTag", "root")
      .option("inferSchema", true)
      .xml(mixedChildren2)
    assert(mixedDF.select("foo.bar").head().getString(0) === " lorem ")
    assert(mixedDF.select("foo.baz.bing").head().getLong(0) === 2)
    assert(mixedDF.select("missing").head().getString(0) === " ipsum ")
  }

}
