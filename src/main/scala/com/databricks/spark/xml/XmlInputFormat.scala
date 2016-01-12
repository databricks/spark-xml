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

import java.io.{InputStream, IOException}

import org.apache.commons.io.Charsets
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Seekable
import org.apache.hadoop.io.compress._
import org.apache.hadoop.io.{DataOutputBuffer, LongWritable, Text}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}

/**
 * Reads records that are delimited by a specific start/end tag.
 */
class XmlInputFormat extends TextInputFormat {

  override def createRecordReader(
      split: InputSplit,
      context: TaskAttemptContext): RecordReader[LongWritable, Text] = {
    new XmlRecordReader
  }
}

object XmlInputFormat {
  /** configuration key for start tag */
  val START_TAG_KEY: String = "xmlinput.start"
  /** configuration key for end tag */
  val END_TAG_KEY: String = "xmlinput.end"
}

/**
 * XMLRecordReader class to read through a given xml document to output xml blocks as records
 * as specified by the start tag and end tag
 */
private[xml] class XmlRecordReader extends RecordReader[LongWritable, Text] {
  private var startTag: Array[Byte] = _
  private var endTag: Array[Byte] = _
  private var space: Byte = _

  private var currentKey: LongWritable = _
  private var currentValue: Text = _

  private var start: Long = _
  private var end: Long = _
  private var in: InputStream = _
  private var filePosition: Seekable = _
  private var decompressor: Decompressor = _

  private val buffer: DataOutputBuffer = new DataOutputBuffer

  def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    val fileSplit: FileSplit = split.asInstanceOf[FileSplit]
    val conf: Configuration = {
      // Use reflection to get the Configuration. This is necessary because TaskAttemptContext is
      // a class in Hadoop 1.x and an interface in Hadoop 2.x.
      val method = context.getClass.getMethod("getConfiguration")
      method.invoke(context).asInstanceOf[Configuration]
    }
    startTag = conf.get(XmlInputFormat.START_TAG_KEY).getBytes(Charsets.UTF_8)
    endTag = conf.get(XmlInputFormat.END_TAG_KEY).getBytes(Charsets.UTF_8)
    space = ' '
    require(startTag != null, "The start tag cannot be null.")
    require(endTag != null, "The end tag cannot be null.")
    start = fileSplit.getStart
    end = start + fileSplit.getLength

    // open the file and seek to the start of the split
    val path = fileSplit.getPath
    val fs = path.getFileSystem(conf)
    val fsin = fs.open(fileSplit.getPath)

    val codec = new CompressionCodecFactory(conf).getCodec(path)
    if (null != codec) {
      decompressor = CodecPool.getDecompressor(codec)
      // Use reflection to get the splittable compression stream. This is necessary
      // because SplittableCompressionCodec does not exist in Hadoop 1.0.x.
      def isSplitCompressionCodec(obj: Any) = {
        val splittableClassName = "org.apache.hadoop.io.compress.SplittableCompressionCodec"
        val test = obj.getClass.getInterfaces.map(_.getName)
        obj.getClass.getInterfaces.map(_.getName).contains(splittableClassName)
      }
      codec match {
        case c: CompressionCodec if isSplitCompressionCodec(c) =>
          val cIn = {
            val method = c.getClass.getMethod("createInputStream",
              classOf[InputStream],
              classOf[Decompressor],
              classOf[Long],
              classOf[Long],
              classOf[SplittableCompressionCodec.READ_MODE])
            method.invoke(c, fsin, decompressor, start.asInstanceOf[AnyRef],
              end.asInstanceOf[AnyRef], SplittableCompressionCodec.READ_MODE.BYBLOCK)
          }.asInstanceOf[CompressionInputStream]
          start = {
            val method = cIn.getClass.getMethod("getAdjustedStart")
            method.invoke(cIn).asInstanceOf[Long]
          }
          end = {
            val method = cIn.getClass.getMethod("getAdjustedEnd")
            method.invoke(cIn).asInstanceOf[Long]
          }
          in = cIn
          filePosition = cIn
        case c: CompressionCodec =>
          if (start != 0) {
            // So we have a split that is only part of a file stored using
            // a Compression codec that cannot be split.
            throw new IOException("Cannot seek in " +
              codec.getClass.getSimpleName + " compressed stream")
          }
          in = c.createInputStream(fsin, decompressor)
          filePosition = fsin
      }
    } else {
      in = fsin
      filePosition = fsin
      filePosition.seek(start)
    }
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
    if (readUntilMatch(startTag, withinBlock = false)) {
      try {
        buffer.write(startTag)
        if (readUntilMatch(endTag, withinBlock = true)) {
          key.set(filePosition.getPos)
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
      val b: Int = in.read
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
          if (b == space && !withinBlock) {
            startTag(startTag.length - 1) = space
            return true
          }
        }
        i = 0
      }
      if (!withinBlock && i == 0 && filePosition.getPos > end) {
        return false
      }
    }
    false
  }

  override def getProgress: Float = (filePosition.getPos - start) / (end - start).toFloat

  override def getCurrentKey: LongWritable = currentKey

  override def getCurrentValue: Text = currentValue

  def close(): Unit = {
    try {
      if (in != null) {
        in.close()
      }
    } finally {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
        decompressor = null
      }
    }
  }
}
