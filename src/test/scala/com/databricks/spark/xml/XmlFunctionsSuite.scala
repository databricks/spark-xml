package com.databricks.spark.xml

import java.nio.file.Files

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class XmlFunctionsSuite extends FunSuite with BeforeAndAfterAll {

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

  test("roundtrip") {

    val xmlData =
      s"""
         |   <parent><pid>14ft3</pid>
         |      <name>dave guy</name>
         |   </parent>
       """.stripMargin

    val schema: StructType = new StructType().add("pid", StringType).add("name", StringType)

    val df: DataFrame = sqlContext.createDataFrame(
      sqlContext.sparkContext.parallelize(List(Row(8, xmlData))),
      StructType(Seq(StructField("number", IntegerType, true),
        StructField("payload", StringType, true))))

    df.withColumn("decoded",
      from_xml(df.col("payload"),
        schema, Map("rootTag" -> "parent")))
      .show()
  }

}
