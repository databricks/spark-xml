/*
 * Copyright 2014 Databricks
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

import org.apache.spark.sql.Row

/**
 * Exception thrown when the underlying parser returns a partial result of parsing.
 * @param partialResult the partial result of parsing a bad record.
 * @param cause the actual exception about why the parser cannot return full result.
 */
case class PartialResultException(
    partialResult: Row,
    cause: Throwable)
  extends Exception(cause)
