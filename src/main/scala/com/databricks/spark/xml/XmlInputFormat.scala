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
package com.databricks.spark.xml

import org.apache.commons.io.Charsets
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.io.{DataOutputBuffer, LongWritable, Text}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}

/**
 * Reads records that are delimited by a specific begin/end tag.
 */
class XmlInputFormat extends TextInputFormat {

  override def createRecordReader(
      split: InputSplit,
      context: TaskAttemptContext): RecordReader[LongWritable, Text] = {
    new XmlRecordReader
  }
}

object XmlInputFormat {

  /** configuration key for start-tag */
  val START_TAG_KEY: String = "xmlinput.start"
  /** configuration key for end-tag */
  val END_TAG_KEY: String = "xmlinput.end"
}

/**
 * XMLRecordReader class to read through a given xml document to output xml blocks as records
 * as specified by the start tag and end tag
 */
private[xml] class XmlRecordReader extends RecordReader[LongWritable, Text] {
  private var startTag: Array[Byte] = _
  private var endTag: Array[Byte] = _

  private var currentKey: LongWritable = _
  private var currentValue: Text = _

  private var start: Long = _
  private var end: Long = _
  private var fsin: FSDataInputStream = _

  private val buffer: DataOutputBuffer = new DataOutputBuffer

  def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    val fileSplit: FileSplit = split.asInstanceOf[FileSplit]
    val conf: Configuration = {
      // Use reflection to get the Configuration. This is necessary because TaskAttemptContext is
      // a class in Hadoop 1.x and an interface in Hadoop 2.x.
      val method = context.getClass.getMethod("getConfiguration")
      method.invoke(context).asInstanceOf[Configuration]
    }
    val fs = fileSplit.getPath.getFileSystem(conf)
    startTag = conf.get(XmlInputFormat.START_TAG_KEY).getBytes(Charsets.UTF_8)
    endTag = conf.get(XmlInputFormat.END_TAG_KEY).getBytes(Charsets.UTF_8)
    require(startTag != null, "The start-tag cannot be null.")
    require(endTag != null, "The end-tag cannot be null.")
    start = fileSplit.getStart
    end = start + fileSplit.getLength
    fsin = fs.open(fileSplit.getPath)
    fsin.seek(start)
  }

  override def nextKeyValue: Boolean = {
    currentKey = new LongWritable
    currentValue = new Text
    next(currentKey, currentValue)
  }

  /**
   * Finds the start of the next record.
   * It treats data from `startTag` and `endTag` as a record.
   *
   * @param key the current key that will be written
   * @param value  the object that will be written
   * @return whether it reads successfully
   */
  private def next(key: LongWritable, value: Text): Boolean = {
    if (fsin.getPos < end && readUntilMatch(startTag, withinBlock = false)) {
      try {
        buffer.write(startTag)
        if (readUntilMatch(endTag, withinBlock = true)) {
          key.set(fsin.getPos)
          value.set(buffer.getData, 0, buffer.getLength)
          true
        } else {
          false
        }
      } finally {
        buffer.reset
      }
    } else {
      false
    }
  }

  /**
   * Read until the given data are matched with `mat`.
   * When withinBlock is true, it saves the data came in.
   *
   * @param mat bytes to match
   * @param withinBlock start offset
   * @return whether it finds the match successfully
   */
  private def readUntilMatch(mat: Array[Byte], withinBlock: Boolean): Boolean = {
    var i: Int = 0
    while (true) {
      val b: Int = fsin.read
      if (b == -1) {
        return false
      }
      if (withinBlock) {
        buffer.write(b)
      }
      if (b == mat(i)) {
        i += 1
        if (i >= mat.length) {
          return true
        }
      }
      else {
        if (i == (mat.length - 1)) {
          if (b == ' ' && !withinBlock) {
            startTag(startTag.length - 1) = ' '
            return true
          }
        }
        i = 0
      }
      if (!withinBlock && i == 0 && fsin.getPos >= end) {
        return false
      }
    }
    false
  }

  override def getProgress: Float = (fsin.getPos - start) / (end - start).toFloat

  override def getCurrentKey: LongWritable = currentKey

  override def getCurrentValue: Text = currentValue

  def close(): Unit = fsin.close()
}
