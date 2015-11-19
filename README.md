# Spark XML Library [![Build Status](https://travis-ci.org/HyukjinKwon/spark-xml.svg?branch=master)](https://travis-ci.org/HyukjinKwon/spark-xml)

- A library for parsing and querying XML data with Apache Spark, for Spark SQL and DataFrames.
The structure and test tools are mostly copied from databricks/spark-csv.

- This package supports to process format-free XML files in a distributed way, unlike JSON datasource in Spark restricts in-line JSON format.

- Since this package is supposed to move under another organazation, it is not uploaded to maven yet.

## Requirements

This library requires Spark 1.3+

## Using with Spark shell
This package can be added to  Spark using the `--packages` command line option.  For example, to include it when starting the spark shell:

### Spark compiled with Scala 2.11
```
$SPARK_HOME/bin/spark-shell --packages HyukjinKwon:spark-xml:0.1.1-s_2.11
```

### Spark compiled with Scala 2.10
```
$SPARK_HOME/bin/spark-shell --packages HyukjinKwon:spark-xml:0.1.1-s_2.10
```

## Features
This package allows reading XML files in local or distributed filesystem as [Spark DataFrames](https://spark.apache.org/docs/1.3.0/sql-programming-guide.html).
When reading files the API accepts several options:
* `path`: location of files. Similar to Spark can accept standard Hadoop globbing expressions.
* `rootTag`: **This is a necessary option.** The root tag of your xml files to treat as a row. For example, in this xml `<books> <book><book> ...</books>`, the appropriate value would be `book`.
* `samplingRatio`: Sampling ratio for inferring schema (0.0 ~ 1). Default is 1. Possible types are `StructType`, `ArrayType`, `StringType`, `LongType`, `DoubleType` and `NullType`, unless user provides a schema for this.
* `excludeAttributeFlag` : Whether you want to exclude tags of elements as fields or not. Default is false.
* `treatEmptyValuesAsNulls` : Whether you want to treat whitespaces as a null value. Default is false.
* `mode`: determines the parsing mode. **This is under development.**
* `charset`: defaults to 'UTF-8' but can be set to other valid charset names. **This is under development. For now, UTF-8 is only supported by default.**

The package does not support to write a Dataframe to XML file.

Currently it supports the shorten name useage. You can use just `xml` instead of `org.apache.spark.sql.xml` from Spark 1.5.0+

These examples use a XML file available for download [here](https://github.com/HyukjinKwon/spark-xml/raw/master/src/test/resources/books.xml):

```
$ wget https://github.com/HyukjinKwon/spark-xml/raw/master/src/test/resources/books.xml
```

### SQL API

Spark-xml can infer data types:
```sql
CREATE TABLE books
USING org.apache.spark.sql.xml
OPTIONS (path "books.xml", rootTag "book")
```

You can also specify column names and types in DDL. In this case, we do not infer schema.
```sql
CREATE TABLE books (author string, description string, genre string, id string, price double, publish_date string, title string)
USING org.apache.spark.sql.xml
OPTIONS (path "books.xml", rootTag "book")
```

### Scala API
__Spark 1.4+:__

```scala
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
val df = sqlContext.read
    .format("org.apache.spark.sql.xml")
    .option("rootTag", "book") // This should be always given.
    .load("books.xml")

df.collect().foreach(println)
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
    .format("org.apache.spark.sql.xml")
    .option("rootTag", "book") // This should be always given.
    .schema(customSchema)
    .load("books.xml")

df.select("author", "id").collect().foreach(println)
```


__Spark 1.3:__

```scala
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
val df = sqlContext.load(
    "org.apache.spark.sql.xml", 
    Map("path" -> "books.xml", "rootTag" -> "book"))

df.select("author", "id").collect().foreach(println)
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
    "org.apache.spark.sql.xml", 
    schema = customSchema,
    Map("path" -> "books.xml", "rootTag" -> "book"))

df.select("author", "id").collect().foreach(println)
```

### Java API
__Spark 1.4+:__

```java
import org.apache.spark.sql.SQLContext

SQLContext sqlContext = new SQLContext(sc);
DataFrame df = sqlContext.read()
    .format("org.apache.spark.sql.xml")
    .option("rootTag", "book")
    .load("books.xml");

df.select("author", "id").collect();
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
    .format("org.apache.spark.sql.xml")
    .option("rootTag", "book")
    .schema(customSchema)
    .load("books.xml");

df.select("author", "id").collect();
```



__Spark 1.3:__

```java
import org.apache.spark.sql.SQLContext

SQLContext sqlContext = new SQLContext(sc);

HashMap<String, String> options = new HashMap<String, String>();
options.put("rootTag", "book");
options.put("path", "books.xml");

DataFrame df = sqlContext.load("org.apache.spark.sql.xml", options);
df.select("author", "id").collect();
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
options.put("rootTag", "book");
options.put("path", "books.xml");

DataFrame df = sqlContext.load("org.apache.spark.sql.xml", customSchema, options);
df.select("author", "id").collect();
```

### Python API

__Spark 1.4+:__

```python
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.read.format('org.apache.spark.sql.xml').options(rootTag='book').load('books.xml')
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
    .format('org.apache.spark.sql.xml') \
    .options(rootTag='book') \
    .load('books.xml', schema = customSchema)

df.select('year', 'model').collect()
```


__Spark 1.3:__

```python
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.load(source="org.apache.spark.xml", rootTag = 'book', path = 'books.xml')
df.select("author", "id").collect()
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

df = sqlContext.load(source="org.apache.spark.xml", rootTag = 'book', schema = customSchema, path = 'books.xml')
df.select("author", "id").collect()
```

## Building From Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html), which is automatically downloaded by the included shell script. To build a JAR file simply run `sbt/sbt package` from the project root. The build configuration includes support for both Scala 2.10 and 2.11.
