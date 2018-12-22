# XML Data Source for Apache Spark

[![Build Status](https://travis-ci.org/databricks/spark-xml.svg?branch=master)](https://travis-ci.org/databricks/spark-xml) [![codecov](https://codecov.io/gh/databricks/spark-xml/branch/master/graph/badge.svg)](https://codecov.io/gh/databricks/spark-xml)

- A library for parsing and querying XML data with Apache Spark, for Spark SQL and DataFrames.
The structure and test tools are mostly copied from [CSV Data Source for Spark](https://github.com/databricks/spark-csv).

- This package supports to process format-free XML files in a distributed way, unlike JSON datasource in Spark restricts in-line JSON format.


## Requirements

This library requires Spark 2.2+ for 0.5.x.

For Spark 2.0.x 2.1.x, use version 0.4.x.
For a version that works with Spark 1.x, please check for [branch-0.3](https://github.com/databricks/spark-xml/tree/branch-0.3).

## Linking
You can link against this library in your program at the following coordinates:

### Scala 2.11

```
groupId: com.databricks
artifactId: spark-xml_2.11
version: 0.5.0
```

### Scala 2.12

```
groupId: com.databricks
artifactId: spark-xml_2.12
version: 0.5.0
```

## Using with Spark shell
This package can be added to Spark using the `--packages` command line option. For example, to include it when starting the spark shell:


### Spark compiled with Scala 2.11
```
$SPARK_HOME/bin/spark-shell --packages com.databricks:spark-xml_2.11:0.5.0
```

### Spark compiled with Scala 2.12
```
$SPARK_HOME/bin/spark-shell --packages com.databricks:spark-xml_2.12:0.5.0
```

## Features
This package allows reading XML files in local or distributed filesystem as [Spark DataFrames](https://spark.apache.org/docs/1.6.0/sql-programming-guide.html).
When reading files the API accepts several options:
* `path`: Location of files. Similar to Spark can accept standard Hadoop globbing expressions.
* `rowTag`: The row tag of your xml files to treat as a row. For example, in this xml `<books> <book><book> ...</books>`, the appropriate value would be `book`. Default is `ROW`. At the moment, rows containing self closing xml tags are not supported.
* `samplingRatio`: Sampling ratio for inferring schema (0.0 ~ 1). Default is 1. Possible types are `StructType`, `ArrayType`, `StringType`, `LongType`, `DoubleType`, `BooleanType`, `TimestampType` and `NullType`, unless user provides a schema for this.
* `excludeAttribute` : Whether you want to exclude attributes in elements or not. Default is false.
* `treatEmptyValuesAsNulls` : (DEPRECATED: use `nullValue` set to `""`) Whether you want to treat whitespaces as a null value. Default is false
* `mode`: The mode for dealing with corrupt records during parsing. Default is `PERMISSIVE`.
  * `PERMISSIVE` :
    * When it encounters a corrupted record, it sets all fields to `null` and puts the malformed string into a new field configured by `columnNameOfCorruptRecord`.
    * When it encounters a field of the wrong datatype, it sets the offending field to `null`.
  * `DROPMALFORMED` : ignores the whole corrupted records.
  * `FAILFAST` : throws an exception when it meets corrupted records.
* `columnNameOfCorruptRecord`: The name of new field where malformed strings are stored. Default is `_corrupt_record`.
* `attributePrefix`: The prefix for attributes so that we can differentiate attributes and elements. This will be the prefix for field names. Default is `_`.
* `valueTag`: The tag used for the value when there are attributes in the element having no child. Default is `_VALUE`.
* `charset`: Defaults to 'UTF-8' but can be set to other valid charset names
* `ignoreSurroundingSpaces`: Defines whether or not surrounding whitespaces from values being read should be skipped. Default is false.

When writing files the API accepts several options:
* `path`: Location to write files.
* `rowTag`: The row tag of your xml files to treat as a row. For example, in this xml `<books> <book><book> ...</books>`, the appropriate value would be `book`. Default is `ROW`.
* `rootTag`: The root tag of your xml files to treat as the root. For example, in this xml `<books> <book><book> ...</books>`, the appropriate value would be `books`. Default is `ROWS`.
* `nullValue`: The value to write `null` value. Default is string `null`. When this is `null`, it does not write attributes and elements for fields.
* `attributePrefix`: The prefix for attributes so that we can differentiating attributes and elements. This will be the prefix for field names. Default is `_`.
* `valueTag`: The tag used for the value when there are attributes in the element having no child. Default is `_VALUE`.
* `compression`: compression codec to use when saving to file. Should be the fully qualified name of a class implementing `org.apache.hadoop.io.compress.CompressionCodec` or one of case-insensitive shorten names (`bzip2`, `gzip`, `lz4`, and `snappy`). Defaults to no compression when a codec is not specified.

Currently it supports the shortened name usage. You can use just `xml` instead of `com.databricks.spark.xml`.

## Structure Conversion

Due to the structure differences between `DataFrame` and XML, there are some conversion rules from XML data to `DataFrame` and from `DataFrame` to XML data. Note that handling attributes can be disabled with the option `excludeAttribute`.


### Conversion from XML to `DataFrame`

- __Attributes__: Attributes are converted as fields with the heading prefix, `attributePrefix`.

    ```xml
    <one myOneAttrib="AAAA">
        <two>two</two>
        <three>three</three>
    </one>
    ```
    produces a schema below:

    ```
    root
     |-- _myOneAttrib: string (nullable = true)
     |-- two: string (nullable = true)
     |-- three: string (nullable = true)
    ```

- __Value in an element that has no child elements but attributes__: The value is put in a separate field, `valueTag`.

    ```xml
    <one>
        <two myTwoAttrib="BBBBB">two</two>
        <three>three</three>
    </one>
    ```
    produces a schema below:
    ```
    root
     |-- two: struct (nullable = true)
     |    |-- _VALUE: string (nullable = true)
     |    |-- _myTwoAttrib: string (nullable = true)
     |-- three: string (nullable = true)
    ```

### Conversion from `DataFrame` to XML

- __Element as an array in an array__:  Writing a XML file from `DataFrame` having a field `ArrayType` with its element as `ArrayType` would have an additional nested field for the element. This would not happen in reading and writing XML data but writing a `DataFrame` read from other sources. Therefore, roundtrip in reading and writing XML files has the same structure but writing a `DataFrame` read from other sources is possible to have a different structure.

    `DataFrame` with a schema below:
    ```
     |-- a: array (nullable = true)
     |    |-- element: array (containsNull = true)
     |    |    |-- element: string (containsNull = true)
    ```

    with data below:
    ```
    +------------------------------------+
    |                                   a|
    +------------------------------------+
    |[WrappedArray(aa), WrappedArray(bb)]|
    +------------------------------------+
    ```

    produces a XML file below:
    ```xml
    <a>
        <item>aa</item>
    </a>
    <a>
        <item>bb</item>
    </a>
    ```


## Examples

These examples use a XML file available for download [here](https://github.com/databricks/spark-xml/raw/master/src/test/resources/books.xml):

```
$ wget https://github.com/databricks/spark-xml/raw/master/src/test/resources/books.xml
```

### SQL API

XML data source for Spark can infer data types:
```sql
CREATE TABLE books
USING com.databricks.spark.xml
OPTIONS (path "books.xml", rowTag "book")
```

You can also specify column names and types in DDL. In this case, we do not infer schema.
```sql
CREATE TABLE books (author string, description string, genre string, _id string, price double, publish_date string, title string)
USING com.databricks.spark.xml
OPTIONS (path "books.xml", rowTag "book")
```

### Scala API

Import `com.databricks.spark.xml._` to get implicits that add the `.xml(...)` method to `DataFrame`.
You can also use `.format("xml")` and `.load(...)`.

```scala
import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._

val spark = SparkSession.builder.getOrCreate()
val df = spark.read
  .option("rowTag", "book")
  .xml("books.xml")

val selectedData = df.select("author", "_id")
selectedData.write
  .option("rootTag", "books")
  .option("rowTag", "book")
  .xml("newbooks.xml")
```

You can manually specify the schema when reading data:

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType}
import com.databricks.spark.xml._

val spark = SparkSession.builder.getOrCreate()
val customSchema = StructType(Array(
  StructField("_id", StringType, nullable = true),
  StructField("author", StringType, nullable = true),
  StructField("description", StringType, nullable = true),
  StructField("genre", StringType, nullable = true),
  StructField("price", DoubleType, nullable = true),
  StructField("publish_date", StringType, nullable = true),
  StructField("title", StringType, nullable = true)))


val df = spark.read
  .option("rowTag", "book")
  .schema(customSchema)
  .xml("books.xml")

val selectedData = df.select("author", "_id")
selectedData.write
  .option("rootTag", "books")
  .option("rowTag", "book")
  .xml("newbooks.xml")
```

### Java API

```java
import org.apache.spark.sql.SparkSession;

SparkSession spark = SparkSession.builder().getOrCreate();
DataFrame df = spark.read()
  .format("xml")
  .option("rowTag", "book")
  .load("books.xml");

df.select("author", "_id").write()
  .format("xml")
  .option("rootTag", "books")
  .option("rowTag", "book")
  .save("newbooks.xml");
```

You can manually specify schema:
```java
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

SparkSession spark = SparkSession.builder().getOrCreate();
StructType customSchema = new StructType(new StructField[] {
  new StructField("_id", DataTypes.StringType, true, Metadata.empty()),
  new StructField("author", DataTypes.StringType, true, Metadata.empty()),
  new StructField("description", DataTypes.StringType, true, Metadata.empty()),
  new StructField("genre", DataTypes.StringType, true, Metadata.empty()),
  new StructField("price", DataTypes.DoubleType, true, Metadata.empty()),
  new StructField("publish_date", DataTypes.StringType, true, Metadata.empty()),
  new StructField("title", DataTypes.StringType, true, Metadata.empty())
});

DataFrame df = spark.read()
  .format("xml")
  .option("rowTag", "book")
  .schema(customSchema)
  .load("books.xml");

df.select("author", "_id").write()
  .format("xml")
  .option("rootTag", "books")
  .option("rowTag", "book")
  .save("newbooks.xml");
```

### Python API

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df = spark.read.format('xml').options(rowTag='book').load('books.xml')
df.select("author", "_id").write \
    .format('xml') \
    .options(rowTag='book', rootTag='books') \
    .save('newbooks.xml')
```

You can manually specify schema:
```python
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()
customSchema = StructType([ \
    StructField("_id", StringType(), True), \
    StructField("author", StringType(), True), \
    StructField("description", StringType(), True), \
    StructField("genre", StringType(), True), \
    StructField("price", DoubleType(), True), \
    StructField("publish_date", StringType(), True), \
    StructField("title", StringType(), True)])

df = spark.read \
    .format('xml') \
    .options(rowTag='book') \
    .load('books.xml', schema = customSchema)

df.select("author", "_id").write \
    .format('xml') \
    .options(rowTag='book', rootTag='books') \
    .save('newbooks.xml')
```

### R API

Automatically infer schema (data types)
```R
library(SparkR)

sparkR.session("local[4]", sparkPackages = c("com.databricks:spark-xml_2.10:0.5"))

df <- read.df("books.xml", source = "xml", rowTag = "book")

# In this case, `rootTag` is set to "ROWS" and `rowTag` is set to "ROW".
write.df(df, "newbooks.csv", "xml", "overwrite")
```

You can manually specify schema:
```R
library(SparkR)

sparkR.session("local[4]", sparkPackages = c("com.databricks:spark-xml_2.10:0.5"))
customSchema <- structType(
  structField("_id", "string"),
  structField("author", "string"),
  structField("description", "string"),
  structField("genre", "string"),
  structField("price", "double"),
  structField("publish_date", "string"),
  structField("title", "string"))

df <- read.df("books.xml", source = "xml", schema = customSchema, rowTag = "book")

# In this case, `rootTag` is set to "ROWS" and `rowTag` is set to "ROW".
write.df(df, "newbooks.csv", "xml", "overwrite")
```

## Hadoop InputFormat

The library contains a Hadoop input format for reading XML files by a start tag and an end tag. This is similar with [XmlInputFormat.java](https://github.com/apache/mahout/blob/9d14053c80a1244bdf7157ab02748a492ae9868a/integration/src/main/java/org/apache/mahout/text/wikipedia/XmlInputFormat.java) in [Mahout](https://mahout.apache.org) but supports to read compressed files, different encodings and read elements including attributes,
which you may make direct use of as follows:

```scala
import com.databricks.spark.xml.XmlInputFormat
import org.apache.spark.SparkContext;
import org.apache.hadoop.io.{LongWritable, Text}

val sc: SparkContext = _

// This will detect the tags including attributes
sc.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, "<book>")
sc.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, "</book>")

val records = sc.newAPIHadoopFile(
  "path",
  classOf[XmlInputFormat],
  classOf[LongWritable],
  classOf[Text])
```

## Building From Source

This library is built with [SBT](https://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html). To build a JAR file simply run `sbt package` from the project root. The build configuration includes support for both Scala 2.11 and 2.12.

## Acknowledgements

This project was initially created by [HyukjinKwon](https://github.com/HyukjinKwon) and donated to [Databricks](https://databricks.com).
