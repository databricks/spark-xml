/*
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

import java.io.File

import org.apache.spark.sql.SparkSession
import org.scalatest.{ BeforeAndAfterAll, FunSuite, Matchers }

final class XmlPartitioningSuite extends FunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("XmlPartitioningSuite")
      .config("spark.ui.enabled", false)
      // chosen to partition input file in the middle of rowTag element
      .config("spark.hadoop.fs.local.block.size", 0x2096.toString)
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try {
      spark.stop()
    } finally {
      super.afterAll()
    }
    spark = null
  }

  test("Fails when file is partitioned in the middle of a rowTag element") {
    val xmlFile = new File(this.getClass.getClassLoader.getResource("fias_house.xml").getFile)

    val results = spark.read
      .option("rowTag", "House")
      .option("mode", "FAILFAST")
      .format("xml")
      .load(xmlFile.getAbsolutePath)

    results.count() shouldBe 37
  }
}
