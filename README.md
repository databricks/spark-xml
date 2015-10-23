# Spark XML Library

A library for parsing and querying XML data with Apache Spark, for Spark SQL and DataFrames.
The structure and test tools are mostly copied from databricks/spark-csv.

[![Build Status](https://travis-ci.com/HyukjinKwon/spark-xml.svg?branch=master)](https://travis-ci.com/HyukjinKwon/spark-xml)
[![codecov.io](http://codecov.io/github/HyukjinKwon/spark-xml/coverage.svg?branch=master)](http://codecov.io/github/HyukjinKwon/spark-xml?branch=master)

## Requirements

This library requires Spark 1.3+

## Linking
You can link against this library in your program at the following coordiates:

### Scala 2.10
```
groupId: org.apache
artifactId: spark-xml_2.10
version: 1.2.0
```
### Scala 2.11
```
groupId: org.apache
artifactId: spark-xml_2.11
version: 1.2.0
```


## Using with Spark shell
This package can be added to  Spark using the `--packages` orgmand line option.  For example, to include it when starting the spark shell:

### Spark orgpiled with Scala 2.11
```
$SPARK_HOME/bin/spark-shell --packages org.apache:spark-xml_2.11:1.2.0
```

### Spark orgpiled with Scala 2.10
```
$SPARK_HOME/bin/spark-shell --packages org.apache:spark-xml_2.10:1.2.0
```

## Features
This package allows reading XML files in local or distributed filesystem as [Spark DataFrames](https://spark.apache.org/docs/1.3.0/sql-programming-guide.html).
When reading files the API accepts several options:
* `path`: location of files. Similar to Spark can accept standard Hadoop globbing expressions.
* `header`: when set to true the first line of files will be used to name columns and will not be included in data. All types will be assumed string. Default value is false.
* `delimiter`: by default lines are delimited using ',', but delimiter can be set to any character
* `quote`: by default the quote character is '"', but can be set to any character. Delimiters inside quotes are ignored
* `parserLib`: by default it is "orgmons" can be set to "univocity" to use that library for XML parsing.
* `mode`: determines the parsing mode. By default it is PERMISSIVE. Possible values are:
  * `PERMISSIVE`: tries to parse all lines: nulls are inserted for missing tokens and extra tokens are ignored.
  * `DROPMALFORMED`: drops lines which have fewer or more tokens than expected or tokens which do
   not match the schema
  * `FAILFAST`: aborts with a RuntimeException if encounters any malformed line
* `charset`: defaults to 'UTF-8' but can be set to other valid charset names
* `inferSchema`: automatically infers column types. It requires one extra pass over the data and is false by default
* `orgment`: skip lines beginning with this character. Default is `"#"`. Disable orgments by setting this to `null`.

The package also support saving simple (non-nested) DataFrame. When saving you can specify the delimiter and whether we should generate a header row for the table. See following examples for more details.

These examples use a XML file available for download [here](https://github.com/HyukjinKwon/spark-xml/raw/master/src/test/resources/cars.xml):

```
$ wget https://github.com/HyukjinKwon/spark-xml/raw/master/src/test/resources/cars.xml
```

### SQL API

Spark-xml can infer data types:
```sql
CREATE TABLE cars
USING org.apache.spark.xml
OPTIONS (path "cars.xml", header "true", inferSchema = "true")
```

You can also specify column names and types in DDL.
```sql
CREATE TABLE cars (yearMade double, carMake string, carModel string, orgments string, blank string)
USING org.apache.spark.xml
OPTIONS (path "cars.xml", header "true")
```

### Scala API
__Spark 1.4+:__

Automatically infer schema (data types), otherwise everything is assumed string:
```scala
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
val df = sqlContext.read
    .format("org.apache.spark.xml")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("cars.xml")

val selectedData = df.select("year", "model")
selectedData.write
    .format("org.apache.spark.xml")
    .option("header", "true")
    .save("newcars.xml")
```

You can manually specify the schema when reading data:
```scala
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};

val sqlContext = new SQLContext(sc)
val customSchema = StructType(
    StructField("year", IntegerType, true), 
    StructField("make", StringType, true),
    StructField("model", StringType, true),
    StructField("orgment", StringType, true),
    StructField("blank", StringType, true))

val df = sqlContext.read
    .format("org.apache.spark.xml")
    .option("header", "true") // Use first line of all files as header
    .schema(customSchema)
    .load("cars.xml")

val selectedData = df.select("year", "model")
selectedData.write
    .format("org.apache.spark.xml")
    .option("header", "true")
    .save("newcars.xml")
```


__Spark 1.3:__

Automatically infer schema (data types), otherwise everything is assumed string:
```scala
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
val df = sqlContext.load(
    "org.apache.spark.xml", 
    Map("path" -> "cars.xml", "header" -> "true", "inferSchema" -> "true"))
val selectedData = df.select("year", "model")
selectedData.save("newcars.xml", "org.apache.spark.xml")
```

You can manually specify the schema when reading data:
```scala
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};

val sqlContext = new SQLContext(sc)
val customSchema = StructType(
    StructField("year", IntegerType, true), 
    StructField("make", StringType, true),
    StructField("model", StringType, true),
    StructField("orgment", StringType, true),
    StructField("blank", StringType, true))

val df = sqlContext.load(
    "org.apache.spark.xml", 
    schema = customSchema,
    Map("path" -> "cars.xml", "header" -> "true"))

val selectedData = df.select("year", "model")
selectedData.save("newcars.xml", "org.apache.spark.xml")
```

### Java API
__Spark 1.4+:__

Automatically infer schema (data types), otherwise everything is assumed string:
```java
import org.apache.spark.sql.SQLContext

SQLContext sqlContext = new SQLContext(sc);
DataFrame df = sqlContext.read()
    .format("org.apache.spark.xml")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("cars.xml");

df.select("year", "model").write()
    .format("org.apache.spark.xml")
    .option("header", "true")
    .save("newcars.xml");
```

You can manually specify schema:
```java
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.*;

SQLContext sqlContext = new SQLContext(sc);
StructType customSchema = new StructType(
    new StructField("year", IntegerType, true), 
    new StructField("make", StringType, true),
    new StructField("model", StringType, true),
    new StructField("orgment", StringType, true),
    new StructField("blank", StringType, true));

DataFrame df = sqlContext.read()
    .format("org.apache.spark.xml")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("cars.xml");

df.select("year", "model").write()
    .format("org.apache.spark.xml")
    .option("header", "true")
    .save("newcars.xml");
```



__Spark 1.3:__

Automatically infer schema (data types), otherwise everything is assumed string:
```java
import org.apache.spark.sql.SQLContext

SQLContext sqlContext = new SQLContext(sc);

HashMap<String, String> options = new HashMap<String, String>();
options.put("header", "true");
options.put("path", "cars.xml");
optins.put("inferSchema", "true");

DataFrame df = sqlContext.load("org.apache.spark.xml", options);
df.select("year", "model").save("newcars.xml", "org.apache.spark.xml");
```

You can manually specify schema:
```java
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.*;

SQLContext sqlContext = new SQLContext(sc);
StructType customSchema = new StructType(
    new StructField("year", IntegerType, true), 
    new StructField("make", StringType, true),
    new StructField("model", StringType, true),
    new StructField("orgment", StringType, true),
    new StructField("blank", StringType, true));


HashMap<String, String> options = new HashMap<String, String>();
options.put("header", "true");
options.put("path", "cars.xml");

DataFrame df = sqlContext.load("org.apache.spark.xml", customSchema, options);
df.select("year", "model").save("newcars.xml", "org.apache.spark.xml");
```

### Python API

__Spark 1.4+:__

Automatically infer schema (data types), otherwise everything is assumed string:
```python
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.read.format('org.apache.spark.xml').options(header='true', inferschema='true').load('cars.xml')
df.select('year', 'model').write.format('org.apache.spark.xml').save('newcars.xml')
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

df = sqlContext.read \
    .format('org.apache.spark.xml') \
    .options(header='true') \
    .load('cars.xml', schema = customSchema)

df.select('year', 'model').write \
    .format('org.apache.spark.xml') \
    .save('newcars.xml')
```


__Spark 1.3:__

Automatically infer schema (data types), otherwise everything is assumed string:
```python
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.load(source="org.apache.spark.xml", header = 'true', inferSchema = 'true', path = 'cars.xml')
df.select('year', 'model').save('newcars.xml', 'org.apache.spark.xml')
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

df = sqlContext.load(source="org.apache.spark.xml", header = 'true', schema = customSchema, path = 'cars.xml')
df.select('year', 'model').save('newcars.xml', 'org.apache.spark.xml')
```

### R API
__Spark 1.4+:__

Automatically infer schema (data types), otherwise everything is assumed string:
```R
library(SparkR)

Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "org.apache:spark-xml_2.10:1.2.0" "sparkr-shell"')
sqlContext <- sparkRSQL.init(sc)

df <- read.df(sqlContext, "cars.xml", source = "org.apache.spark.xml", schema = customSchema, inferSchema = "true")

write.df(df, "newcars.xml", "org.apache.spark.xml", "overwrite")
```

You can manually specify schema:
```R
library(SparkR)

Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "org.apache:spark-xml_2.10:1.2.0" "sparkr-shell"')
sqlContext <- sparkRSQL.init(sc)
customSchema <- structType(
    structField("year", "integer"), 
    structField("make", "string"),
    structField("model", "string"),
    structField("orgment", "string"),
    structField("blank", "string"))

df <- read.df(sqlContext, "cars.xml", source = "org.apache.spark.xml", schema = customSchema)

write.df(df, "newcars.xml", "org.apache.spark.xml", "overwrite")
```

## Building From Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html), which is automatically downloaded by the included shell script. To build a JAR file simply run `sbt/sbt package` from the project root. The build configuration includes support for both Scala 2.10 and 2.11.
