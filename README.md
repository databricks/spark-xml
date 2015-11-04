# Spark XML Library

- A library for parsing and querying XML data with Apache Spark, for Spark SQL and DataFrames.
The structure and test tools are mostly copied from databricks/spark-csv.

- This package supports to process format-free XML files in a distributed way, unlike JSON datasource in Spark restricts in-line JSON format.


## Requirements

This library requires Spark 1.3+

## Features
This package allows reading XML files in local or distributed filesystem as [Spark DataFrames](https://spark.apache.org/docs/1.3.0/sql-programming-guide.html).
When reading files the API accepts several options:
* `path`: location of files. Similar to Spark can accept standard Hadoop globbing expressions.
* `rootTag`: **This is a necessary option.** The root tag of your xml files to treat as a row. For example, in this xml `<books> <book><book> ...</books>`, the appropriate value would be `book`.
* `samplingRatio`: Sampling ratio for infering schema. For now, this always tries to infer schema. Possible types are only `StructType`, `ArrayType`, `StringType`, `LongType`, `DoubleType` and `NullType`, unless user provides a schema for this.
* `includeAttributeFlag` : Whether you want to include tags of elements as fields or not. **This is under development. For now, always includes tags**
* `treatEmptyValuesAsNulls` : Whether you want to treat whitespaces as a null value. **This is under development.**
* `mode`: determines the parsing mode. **This is under development.**
* `charset`: defaults to 'UTF-8' but can be set to other valid charset names. **This is under development. For now, UTF-8 is only supported by default.**

The package does not support to write a Dataframe to XML file.

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
CREATE TABLE cars (yearMade double, carMake string, carModel string, orgments string, blank string)
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
customSchema = StructType( \
    StructField("author", StringType, true), \
    StructField("description", StringType, true), \
    StructField("genre", StringType, true), \
    StructField("id", StringType, true), \
    StructField("price", DoubleType, true), \
    StructField("publish_date", StringType, true), \
    StructField("title", StringType, true)) \

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
customSchema = StructType( \
    StructField("year", IntegerType, true), \
    StructField("make", StringType, true), \
    StructField("model", StringType, true), \
    StructField("orgment", StringType, true), \
    StructField("blank", StringType, true))

df = sqlContext.load(source="org.apache.spark.xml", rootTag = 'book', schema = customSchema, path = 'books.xml')
df.select("author", "id").collect()
```

## Building From Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html), which is automatically downloaded by the included shell script. To build a JAR file simply run `sbt/sbt package` from the project root. The build configuration includes support for both Scala 2.10 and 2.11.
