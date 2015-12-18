# Spark XML Library [![Build Status](https://travis-ci.org/databricks/spark-xml.svg?branch=master)](https://travis-ci.org/databricks/spark-xml) [![codecov.io](http://codecov.io/github/databricks/spark-xml/coverage.svg?branch=master)](http://codecov.io/github/databricks/spark-xml?branch=master)

- A library for parsing and querying XML data with Apache Spark, for Spark SQL and DataFrames.
The structure and test tools are mostly copied from databricks/spark-csv.

- This package supports to process format-free XML files in a distributed way, unlike JSON datasource in Spark restricts in-line JSON format.


## Requirements

This library requires Spark 1.3+

## Features
This package allows reading XML files in local or distributed filesystem as [Spark DataFrames](https://spark.apache.org/docs/1.3.0/sql-programming-guide.html).
When reading files the API accepts several options:
* `path`: location of files. Similar to Spark can accept standard Hadoop globbing expressions.
* `rowTag`: The row tag of your xml files to treat as a row. For example, in this xml `<books> <book><book> ...</books>`, the appropriate value would be `book`. Default is `ROW`.
* `samplingRatio`: Sampling ratio for inferring schema (0.0 ~ 1). Default is 1. Possible types are `StructType`, `ArrayType`, `StringType`, `LongType`, `DoubleType` and `NullType`, unless user provides a schema for this.
* `excludeAttributeFlag` : Whether you want to exclude tags of elements as fields or not. Default is false.
* `treatEmptyValuesAsNulls` : Whether you want to treat whitespaces as a null value. Default is false.
* `mode`: determines the parsing mode. **This is under development.**
* `charset`: defaults to 'UTF-8' but can be set to other valid charset names. **This is under development. For now, UTF-8 is only supported by default.**

When writing files the API accepts several options:
* `path`: location of files. Similar to Spark can accept standard Hadoop globbing expressions.
* `rowTag`: The row tag of your xml files to treat as a row. For example, in this xml `<books> <book><book> ...</books>`, the appropriate value would be `book`. Default is `ROW`.
* `rootTag`: The root tag of your xml files to treat as the root. For example, in this xml `<books> <book><book> ...</books>`, the appropriate value would be `books`. Default is `ROWS`.
* `nullValue`: The value to write `null` value. Default is string `null`.

Currently it supports the shorten name useage. You can use just `xml` instead of `com.databricks.spark.xml` from Spark 1.5.0+

These examples use a XML file available for download [here](https://github.com/databricks/spark-xml/raw/master/src/test/resources/books.xml):

```
$ wget https://github.com/databricks/spark-xml/raw/master/src/test/resources/books.xml
```

### SQL API

Spark-xml can infer data types:
```sql
CREATE TABLE books
USING com.databricks.spark.xml
OPTIONS (path "books.xml", rowTag "book")
```

You can also specify column names and types in DDL. In this case, we do not infer schema.
```sql
CREATE TABLE books (author string, description string, genre string, id string, price double, publish_date string, title string)
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

val selectedData = df.select("author", "id")
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
val customSchema = StructType(
    StructField("author", StringType, nullable = true),
    StructField("description", StringType, nullable = true),
    StructField("genre", StringType ,nullable = true),
    StructField("id", StringType, nullable = true),
    StructField("price", DoubleType, nullable = true),
    StructField("publish_date", StringType, nullable = true),
    StructField("title", StringType, nullable = true))


val df = sqlContext.read
    .format("com.databricks.spark.xml")
    .option("rowTag", "book")
    .schema(customSchema)
    .load("books.xml")

val selectedData = df.select("author", "id")
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

val selectedData = df.select("author", "id")
selectedData.save("com.databricks.spark.xml",
	SaveMode.ErrorIfExists,
	Map("path" -> "newbooks.xml", "rootTag" -> "books", "rowTag" -> "book"))
```

You can manually specify the schema when reading data:
```scala
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};

val sqlContext = new SQLContext(sc)
val customSchema = StructType(
    StructField("author", StringType, nullable = true),
    StructField("description", StringType, nullable = true),
    StructField("genre", StringType ,nullable = true),
    StructField("id", StringType, nullable = true),
    StructField("price", DoubleType, nullable = true),
    StructField("publish_date", StringType, nullable = true),
    StructField("title", StringType, nullable = true))

val df = sqlContext.load(
    "com.databricks.spark.xml",
    schema = customSchema,
    Map("path" -> "books.xml", "rowTag" -> "book"))

val selectedData = df.select("author", "id")
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

df.select("author", "id").write()
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
StructType customSchema = new StructType(
    new StructField("author", StringType, true),
    new StructField("description", StringType, true),
    new StructField("genre", StringType, true),
    new StructField("id", StringType, true),
    new StructField("price", DoubleType, true),
    new StructField("publish_date", StringType, true),
    new StructField("title", StringType, true));

DataFrame df = sqlContext.read()
    .format("com.databricks.spark.xml")
    .option("rowTag", "book")
    .schema(customSchema)
    .load("books.xml");

df.select("author", "id").write()
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
df.select("author", "id").save("com.databricks.spark.xml", SaveMode.ErrorIfExists, options)
```

You can manually specify schema:
```java
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.*;

SQLContext sqlContext = new SQLContext(sc);
StructType customSchema = new StructType(
    new StructField("author", StringType, true),
    new StructField("description", StringType, true),
    new StructField("genre", StringType, true),
    new StructField("id", StringType, true),
    new StructField("price", DoubleType, true),
    new StructField("publish_date", StringType, true),
    new StructField("title", StringType, true));

HashMap<String, String> options = new HashMap<String, String>();
options.put("rowTag", "book");
options.put("path", "books.xml");
DataFrame df = sqlContext.load("com.databricks.spark.xml", customSchema, options);

HashMap<String, String> options = new HashMap<String, String>();
options.put("rowTag", "book");
options.put("rootTag", "books");
options.put("path", "newbooks.xml");
df.select("author", "id").save("com.databricks.spark.xml", SaveMode.ErrorIfExists, options)
```

### Python API

__Spark 1.4+:__

```python
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.xml').options(rowTag='book').load('books.xml')
df.select("author", "id").collect()
```

You can manually specify schema:
```python
from pyspark.sql import SQLContext
from pyspark.sql.types import *

sqlContext = SQLContext(sc)
customSchema = StructType([ \
    StructField("author", StringType(), True), \
    StructField("description", StringType(), True), \
    StructField("genre", StringType(), True), \
    StructField("id", StringType(), True), \
    StructField("price", DoubleType(), True), \
    StructField("publish_date", StringType(), True), \
    StructField("title", StringType(), True])) \

df = sqlContext.read \
    .format('com.databricks.spark.xml') \
    .options(rowTag='book') \
    .load('books.xml', schema = customSchema)

df.select("author", "id").write \
    .format('com.databricks.spark.xml') \
    .options(rowTag='book', rootTag='books') \
    .save('newbooks.xml')
```


__Spark 1.3:__

```python
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.load(source="com.databricks.spark.xml", rowTag = 'book', path = 'books.xml')
df.select("author", "id").save('newbooks.xml', rootTag = 'books', rowTag = 'book', path = 'newbooks.xml')
```

You can manually specify schema:
```python
from pyspark.sql import SQLContext
from pyspark.sql.types import *

sqlContext = SQLContext(sc)
customSchema = StructType([ \
    StructField("year", IntegerType(), True), \
    StructField("make", StringType(), True), \
    StructField("model", StringType(), True), \
    StructField("orgment", StringType(), True), \
    StructField("blank", StringType(), True)])

df = sqlContext.load(source="com.databricks.spark.xml", rowTag = 'book', schema = customSchema, path = 'books.xml')
df.select("author", "id").save('newbooks.xml', rootTag = 'books', rowTag = 'book', path = 'newbooks.xml')
```

## Building From Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html), which is automatically downloaded by the included shell script. To build a JAR file simply run `sbt/sbt package` from the project root. The build configuration includes support for both Scala 2.10 and 2.11.


## Acknowledgements

This project was initially created by [HyukjinKwon](https://github.com/HyukjinKwon) and donated to [Databricks](https://databricks.com).

