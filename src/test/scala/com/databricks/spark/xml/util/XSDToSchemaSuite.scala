/*
 * Copyright 2020 Databricks
 *
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

package com.databricks.spark.xml.util

import java.nio.file.Paths

import org.apache.spark.sql.types.{ArrayType, StructField, StructType, StringType}
import org.scalatest.funsuite.AnyFunSuite

class XSDToSchemaSuite extends AnyFunSuite {

  test("Basic parsing") {
    val parsedSchema = XSDToSchema.read(Paths.get("src/test/resources/basket.xsd"))
    val expectedSchema = StructType(Array(
      StructField("basket", StructType(Array(
        StructField("entry", ArrayType(
          StructType(Array(
            StructField("key", StringType),
            StructField("value", StringType)
          )))
        ))
      )))
    )
    assert(expectedSchema === parsedSchema)
  }

  test("Relative path parsing") {
    val parsedSchema = XSDToSchema.read(
      Paths.get("src/test/resources/include-example/first.xsd"))
    val expectedSchema = StructType(Array(
      StructField("basket", StructType(Array(
        StructField("entry", ArrayType(
          StructType(Array(
            StructField("key", StringType),
            StructField("value", StringType)
          )))
        ))
      )))
    )
    assert(expectedSchema === parsedSchema)
  }
}
