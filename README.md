# XML Data Source for Apache Spark

[![Build Status](https://travis-ci.org/databricks/spark-xml.svg?branch=master)](https://travis-ci.org/databricks/spark-xml) [![codecov.io](http://codecov.io/github/databricks/spark-xml/coverage.svg?branch=master)](http://codecov.io/github/databricks/spark-xml?branch=master)

- A library for parsing and querying XML data with Apache Spark, for Spark SQL and DataFrames.
The structure and test tools are mostly copied from [CSV Data Source for Spark](https://github.com/databricks/spark-csv).

- This package supports to process format-free XML files in a distributed way, unlike JSON datasource in Spark restricts in-line JSON format.


## Requirements

This library requires Spark 1.3+


## Linking
You can link against this library in your program at the following coordinates:

### Scala 2.10
```
groupId: com.databricks
artifactId: spark-xml_2.10
version: 0.3.3
```
### Scala 2.11
```
groupId: com.databricks
artifactId: spark-xml_2.11
version: 0.3.3
```

## Using with Spark shell
This package can be added to  Spark using the `--packages` command line option.  For example, to include it when starting the spark shell:

### Spark compiled with Scala 2.10
```
$SPARK_HOME/bin/spark-shell --packages com.databricks:spark-xml_2.10:0.3.3
```

### Spark compiled with Scala 2.11
```
$SPARK_HOME/bin/spark-shell --packages com.databricks:spark-xml_2.11:0.3.3
```

## Features
This package allows reading XML files in local or distributed filesystem as [Spark DataFrames](https://spark.apache.org/docs/1.6.0/sql-programming-guide.html).
When reading files the API accepts several options:
* `path`: Location of files. Similar to Spark can accept standard Hadoop globbing expressions.
* `rowTag`: The row tag of your xml files to treat as a row. For example, in this xml `<books> <book><book> ...</books>`, the appropriate value would be `book`. Default is `ROW`.
* `samplingRatio`: Sampling ratio for inferring schema (0.0 ~ 1). Default is 1. Possible types are `StructType`, `ArrayType`, `StringType`, `LongType`, `DoubleType`, `BooleanType`, `TimestampType` and `NullType`, unless user provides a schema for this.
* `excludeAttribute` : Whether you want to exclude attributes in elements or not. Default is false.
* `treatEmptyValuesAsNulls` : Whether you want to treat whitespaces as a null value. Default is false.
* `failFast` : Whether you want to fail when it fails to parse malformed rows in XML files, instead of dropping the rows. Default is false.
* `attributePrefix`: The prefix for attributes so that we can differentiate attributes and elements. This will be the prefix for field names. Default is `@`.
* `valueTag`: The tag used for the value when there are attributes in the element having no child. Default is `#VALUE`.
* `charset`: Defaults to 'UTF-8' but can be set to other valid charset names

When writing files the API accepts several options:
* `path`: Location to write files.
* `rowTag`: The row tag of your xml files to treat as a row. For example, in this xml `<books> <book><book> ...</books>`, the appropriate value would be `book`. Default is `ROW`.
* `rootTag`: The root tag of your xml files to treat as the root. For example, in this xml `<books> <book><book> ...</books>`, the appropriate value would be `books`. Default is `ROWS`.
* `nullValue`: The value to write `null` value. Default is string `null`. When this is `null`, it does not write attributes and elements for fields.
* `attributePrefix`: The prefix for attributes so that we can differentiating attributes and elements. This will be the prefix for field names. Default is `@`.
* `valueTag`: The tag used for the value when there are attributes in the element having no child. Default is `#VALUE`.
* `codec`: compression codec to use when saving to file. Should be the fully qualified name of a class implementing `org.apache.hadoop.io.compress.CompressionCodec` or one of case-insensitive shorten names (`bzip2`, `gzip`, `lz4`, and `snappy`). Defaults to no compression when a codec is not specified.

Currently it supports the shortened name usage. You can use just `xml` instead of `com.databricks.spark.xml` from Spark 1.5.0+

## Structure Conversion

Due to the structure differences between `DataFrame` and XML, there are some conversion rules from XML data to `DataFrame` and from `DataFrame` to XML data. Note that handling attributes can be disabled with the option `excludeAttribute`.


### Conversion from XML to `DataFrame`

- __Attributes__: Attributes are converted as fields with the heading prefix, `attributePrefix`.

    ```xml
    ...
    <one myOneAttrib="AAAA">
        <two>two</two>
        <three>three</three>
    </one>
    ...
    ```
    produces a schema below:

    ```
    root
     |-- @myOneAttrib: string (nullable = true)
     |-- two: string (nullable = true)
     |-- three: string (nullable = true)
    ```

- __Value in an element that has no child elements but attributes__: The value is put in a separate field, `valueTag`.

    ```xml
    ...
    <one>
        <two myTwoAttrib="BBBBB">two</two>
        <three>three</three>
    </one>
    ...
    ```
    produces a schema below:
    ```
    root
     |-- two: struct (nullable = true)
     |    |-- #VALUE: string (nullable = true)
     |    |-- @myTwoAttrib: string (nullable = true)
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
    ...
    <a>
        <item>aa</item>
    </a>
    <a>
        <item>bb</item>
    </a>
    ...
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
CREATE TABLE books (author string, description string, genre string, @id string, price double, publish_date string, title string)
USING com.databricks.spark.xml
OPTIONS (path "books.xml", rowTag "book")
```

### Scala API
__Spark 1.4+:__

```scala
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
val df = sqlContext.read
    .format("com.databricks.spark.xml")
    .option("rowTag", "book")
    .load("books.xml")

val selectedData = df.select("author", "@id")
selectedData.write
    .format("com.databricks.spark.xml")
    .option("rootTag", "books")
    .option("rowTag", "book")
    .save("newbooks.xml")
```

You can manually specify the schema when reading data:
```scala
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType};

val sqlContext = new SQLContext(sc)
val customSchema = StructType(Array(
    StructField("@id", StringType, nullable = true),
    StructField("author", StringType, nullable = true),
    StructField("description", StringType, nullable = true),
    StructField("genre", StringType ,nullable = true),
    StructField("price", DoubleType, nullable = true),
    StructField("publish_date", StringType, nullable = true),
    StructField("title", StringType, nullable = true)))


val df = sqlContext.read
    .format("com.databricks.spark.xml")
    .option("rowTag", "book")
    .schema(customSchema)
    .load("books.xml")

val selectedData = df.select("author", "@id")
selectedData.write
    .format("com.databricks.spark.xml")
    .option("rootTag", "books")
    .option("rowTag", "book")
    .save("newbooks.xml")
```

__Spark 1.3:__

```scala
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
val df = sqlContext.load(
    "com.databricks.spark.xml",
    Map("path" -> "books.xml", "rowTag" -> "book"))

val selectedData = df.select("author", "@id")
selectedData.save("com.databricks.spark.xml",
	SaveMode.ErrorIfExists,
	Map("path" -> "newbooks.xml", "rootTag" -> "books", "rowTag" -> "book"))
```

You can manually specify the schema when reading data:
```scala
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};

val sqlContext = new SQLContext(sc)
val customSchema = StructType(Array(
    StructField("@id", StringType, nullable = true),
    StructField("author", StringType, nullable = true),
    StructField("description", StringType, nullable = true),
    StructField("genre", StringType ,nullable = true),
    StructField("price", DoubleType, nullable = true),
    StructField("publish_date", StringType, nullable = true),
    StructField("title", StringType, nullable = true)))

val df = sqlContext.load(
    "com.databricks.spark.xml",
    schema = customSchema,
    Map("path" -> "books.xml", "rowTag" -> "book"))

val selectedData = df.select("author", "@id")
selectedData.save("com.databricks.spark.xml",
	SaveMode.ErrorIfExists,
	Map("path" -> "newbooks.xml", "rootTag" -> "books", "rowTag" -> "book"))
```

### Java API
__Spark 1.4+:__

```java
import org.apache.spark.sql.SQLContext

SQLContext sqlContext = new SQLContext(sc);
DataFrame df = sqlContext.read()
    .format("com.databricks.spark.xml")
    .option("rowTag", "book")
    .load("books.xml");

df.select("author", "@id").write()
    .format("com.databricks.spark.xml")
    .option("rootTag", "books")
    .option("rowTag", "book")
    .save("newbooks.xml");
```

You can manually specify schema:
```java
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.*;

SQLContext sqlContext = new SQLContext(sc);
StructType customSchema = new StructType(new StructField[] {
    new StructField("@id", DataTypes.StringType, true, Metadata.empty()),
    new StructField("author", DataTypes.StringType, true, Metadata.empty()),
    new StructField("description", DataTypes.StringType, true, Metadata.empty()),
    new StructField("genre", DataTypes.StringType, true, Metadata.empty()),
    new StructField("price", DataTypes.DoubleType, true, Metadata.empty()),
    new StructField("publish_date", DataTypes.StringType, true, Metadata.empty()),
    new StructField("title", DataTypes.StringType, true, Metadata.empty())
});

DataFrame df = sqlContext.read()
    .format("com.databricks.spark.xml")
    .option("rowTag", "book")
    .schema(customSchema)
    .load("books.xml");

df.select("author", "@id").write()
    .format("com.databricks.spark.xml")
    .option("rootTag", "books")
    .option("rowTag", "book")
    .save("newbooks.xml");
```



__Spark 1.3:__

```java
import org.apache.spark.sql.SQLContext

SQLContext sqlContext = new SQLContext(sc);

HashMap<String, String> options = new HashMap<String, String>();
options.put("rowTag", "book");
options.put("path", "books.xml");
DataFrame df = sqlContext.load("com.databricks.spark.xml", options);

HashMap<String, String> options = new HashMap<String, String>();
options.put("rowTag", "book");
options.put("rootTag", "books");
options.put("path", "newbooks.xml");
df.select("author", "@id").save("com.databricks.spark.xml", SaveMode.ErrorIfExists, options)
```

You can manually specify schema:
```java
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.*;

SQLContext sqlContext = new SQLContext(sc);
StructType customSchema = new StructType(new StructField[] {
    new StructField("@id", DataTypes.StringType, true, Metadata.empty()),
    new StructField("author", DataTypes.StringType, true, Metadata.empty()),
    new StructField("description", DataTypes.StringType, true, Metadata.empty()),
    new StructField("genre", DataTypes.StringType, true, Metadata.empty()),
    new StructField("price", DataTypes.DoubleType, true, Metadata.empty()),
    new StructField("publish_date", DataTypes.StringType, true, Metadata.empty()),
    new StructField("title", DataTypes.StringType, true, Metadata.empty())
});

HashMap<String, String> options = new HashMap<String, String>();
options.put("rowTag", "book");
options.put("path", "books.xml");
DataFrame df = sqlContext.load("com.databricks.spark.xml", customSchema, options);

HashMap<String, String> options = new HashMap<String, String>();
options.put("rowTag", "book");
options.put("rootTag", "books");
options.put("path", "newbooks.xml");
df.select("author", "@id").save("com.databricks.spark.xml", SaveMode.ErrorIfExists, options)
```

### Python API

__Spark 1.4+:__

```python
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.xml').options(rowTag='book').load('books.xml')
df.select("author", "@id").write \
    .format('com.databricks.spark.xml') \
    .options(rowTag='book', rootTag='books') \
    .save('newbooks.xml')
```

You can manually specify schema:
```python
from pyspark.sql import SQLContext
from pyspark.sql.types import *

sqlContext = SQLContext(sc)
customSchema = StructType([ \
    StructField("@id", StringType(), True), \
    StructField("author", StringType(), True), \
    StructField("description", StringType(), True), \
    StructField("genre", StringType(), True), \
    StructField("price", DoubleType(), True), \
    StructField("publish_date", StringType(), True), \
    StructField("title", StringType(), True)])

df = sqlContext.read \
    .format('com.databricks.spark.xml') \
    .options(rowTag='book') \
    .load('books.xml', schema = customSchema)

df.select("author", "@id").write \
    .format('com.databricks.spark.xml') \
    .options(rowTag='book', rootTag='books') \
    .save('newbooks.xml')
```


__Spark 1.3:__

```python
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.load(source="com.databricks.spark.xml", rowTag = 'book', path = 'books.xml')
df.select("author", "@id").save('newbooks.xml', rootTag = 'books', rowTag = 'book', path = 'newbooks.xml')
```

You can manually specify schema:
```python
from pyspark.sql import SQLContext
from pyspark.sql.types import *

sqlContext = SQLContext(sc)
customSchema = StructType([ \
    StructField("@id", StringType(), True), \
    StructField("author", StringType(), True), \
    StructField("description", StringType(), True), \
    StructField("genre", StringType(), True), \
    StructField("price", DoubleType(), True), \
    StructField("publish_date", StringType(), True), \
    StructField("title", StringType(), True)])

df = sqlContext.load(source="com.databricks.spark.xml", rowTag = 'book', schema = customSchema, path = 'books.xml')
df.select("author", "@id").save('newbooks.xml', rootTag = 'books', rowTag = 'book', path = 'newbooks.xml')
```


### R API
__Spark 1.4+:__

Automatically infer schema (data types)
```R
library(SparkR)

Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.databricks:spark-xml_2.10:0.3.3" "sparkr-shell"')
sqlContext <- sparkRSQL.init(sc)

df <- read.df(sqlContext, "books.xml", source = "com.databricks.spark.xml", rowTag = "book")

# In this case, `rootTag` is set to "ROWS" and `rowTag` is set to "ROW".
write.df(df, "newbooks.csv", "com.databricks.spark.xml", "overwrite")
```

You can manually specify schema:
```R
library(SparkR)

Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.databricks:spark-csv_2.10:0.3.3" "sparkr-shell"')
sqlContext <- sparkRSQL.init(sc)
customSchema <- structType(
    structField("@id", "string"),
    structField("author", "string"),
    structField("description", "string"),
    structField("genre", "string"),
    structField("price", "double"),
    structField("publish_date", "string"),
    structField("title", "string"))

df <- read.df(sqlContext, "books.xml", source = "com.databricks.spark.xml", rowTag = "book")

# In this case, `rootTag` is set to "ROWS" and `rowTag` is set to "ROW".
write.df(df, "newbooks.csv", "com.databricks.spark.xml", "overwrite")
```

## Hadoop InputFormat

The library contains a Hadoop input format for reading XML files by a start tag and an end tag. This is similar with [XmlInputFormat.java](https://github.com/apache/mahout/blob/9d14053c80a1244bdf7157ab02748a492ae9868a/integration/src/main/java/org/apache/mahout/text/wikipedia/XmlInputFormat.java) in [Mahout](http://mahout.apache.org) but supports to read compressed files, different encodings and read elements including attributes,
which you may make direct use of as follows:

```scala
import com.databricks.spark.xml.XmlInputFormat

// This will detect the tags including attributes
sc.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, "<book>")
sc.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, "</book>")
sc.hadoopConfiguration.set(XmlInputFormat.ENCODING_KEY, "utf-8")

val records = sc.newAPIHadoopFile(
  path,
  classOf[XmlInputFormat],
  classOf[LongWritable],
  classOf[Text])
```

## Building From Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html), which is automatically downloaded by the included shell script. To build a JAR file simply run `sbt/sbt package` from the project root. The build configuration includes support for both Scala 2.10 and 2.11.

## Acknowledgements

This project was initially created by [HyukjinKwon](https://github.com/HyukjinKwon) and donated to [Databricks](https://databricks.com).

