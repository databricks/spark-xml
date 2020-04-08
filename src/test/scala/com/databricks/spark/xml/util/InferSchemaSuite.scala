package com.databricks.spark.xml.util

import com.databricks.spark.xml._
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkException
import org.apache.spark.sql.functions.{explode, col}
import org.scalatest.matchers.should.Matchers
import org.apache.spark.rdd.RDD

final class InferSchemaSuite extends FunSuite with BeforeAndAfterAll with Matchers {

  private val schema = (new StructType)
    .add("date", StringType, false)
    .add("message",
      ArrayType(
        (new StructType)
          .add("id", StringType, false)
          .add("sender", StringType, false)
          .add("recipient", StringType, false)
          .add("address", StringType, false)
          .add("content", StringType, false)
          , false
        )
      , false
    )

  private val messages_bad_file = "src/test/resources/messages-bad.xml"

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder
      .master("local[2]")
      .appName("TextFileSuite")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      spark.stop()
      spark = null
    } finally {
      super.afterAll()
    }
  }

  test("load bad xml data should raise exception when action is executed") {
    val messageDF = spark.read
      .option("rowTag", "interchange")
      .schema(schema)
      .xml(messages_bad_file)

    val df = messageDF
      .select(explode(col("message")).as("message"))
      .selectExpr("message.id as id", "message.sender as sender", "message.address as address")

    an [SparkException] should be thrownBy df.count()
  }

  test("load bad xml data should return a empty RDD without raising exception") {
    // In practice this is useful for reading messages from a directory with wholeTextFiles
    // instead of a single file.
    val messageRDD = spark.read.textFile(messages_bad_file).rdd

    case class MatchSchema(
      schema: StructType,
      xmlOptions: Map[String, String] = Map.empty
    ) {
      // This function is complex so, I will leave it out. Just wanted to show the use case
      // of infering the schema from a single XML document in a string
      private def isSubset(left: StructType, right: StructType): Boolean = false

      def filter(rdd: RDD[String]): RDD[String] = {
        rdd.filter { s => isSubset(schema_of_xml_from_string(s), schema) }
      }
    }

    // Filter out the bad files that don't fit the schema
    val goodMessages = MatchSchema(schema).filter(messageRDD)

    goodMessages.count() shouldEqual 0
  }
}
