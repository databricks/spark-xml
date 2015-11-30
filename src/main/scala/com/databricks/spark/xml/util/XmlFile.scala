/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.databricks.spark.xml.util

import java.nio.charset.Charset

import org.apache.hadoop.io.{Text, LongWritable}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.databricks.hadoop.mapreduce.lib.input.XmlInputFormat

private[xml] object XmlFile {
  val DEFAULT_CHARSET = Charset.forName("UTF-8")

  def withCharset(context: SparkContext, location: String,
                  charset: String,
                  rootTag: String): RDD[String] = {
    context.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, s"<$rootTag>")
    context.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, s"</$rootTag>")
    if (Charset.forName(charset) == DEFAULT_CHARSET) {
      context.newAPIHadoopFile(location,
        classOf[XmlInputFormat],
        classOf[LongWritable],
        classOf[Text]).map(pair => new String(pair._2.getBytes, 0, pair._2.getLength))
    } else {
      // can't pass a Charset object here cause its not serializable
      // TODO: maybe use mapPartitions instead?
      context.newAPIHadoopFile(location,
        classOf[XmlInputFormat],
        classOf[LongWritable],
        classOf[Text]).map(pair => new String(pair._2.getBytes, 0, pair._2.getLength, charset))
    }
  }
}
