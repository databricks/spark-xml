package com.databricks.spark.xml.util

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType}

class XmlRDDSuite extends FunSuite with BeforeAndAfterAll {

  private var sqlContext: SQLContext = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(new SparkContext("local[2]", "XmlRDDSuite"))
  }

  override protected def afterAll(): Unit = {
    try {
      sqlContext.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }

  test("empty dataframe is converted to an empty xml rdd") {
    // Check that a dataframe where all partitions are empty is converted
    // to an empty RDD (i.e. doesn't add opening and closing root tags)
    val rows: RDD[Row] = sqlContext.sparkContext.parallelize(Seq[Row]())
    val df = sqlContext.createDataFrame(rows, StructType(Array(StructField("ROW", StringType))))
    val xmlRdd = XmlRDD(df, Map())
    assert(xmlRdd.collect().isEmpty)
  }

  test("RDD contains a string for each row of the dataframe plus opening and closing root tags") {
    val rows = sqlContext.sparkContext.parallelize(Seq(
      Seq("The Cat in the Hat", "Dr Suess"),
      Seq("Where the Wild Things Are", "Maurice Sendak")
    ).map(Row.fromSeq))
    val df = sqlContext.createDataFrame(rows, StructType(Array(
      StructField("title", StringType),
      StructField("author", StringType)
    )))
    val xmlRdd = XmlRDD(df, Map("rootTag" -> "books", "rowTag" -> "book"))
    assert(xmlRdd.partitions.size === 2)
    val partitionArrays = xmlRdd.glom().collect()

    assert(partitionArrays.head === Array("<books>",
      """    <book>
        |        <title>The Cat in the Hat</title>
        |        <author>Dr Suess</author>
        |    </book>""".stripMargin,
        "</books>"))
    assert(partitionArrays(1) === Array("<books>",
      """    <book>
        |        <title>Where the Wild Things Are</title>
        |        <author>Maurice Sendak</author>
        |    </book>""".stripMargin,
        "</books>"))
  }

}
