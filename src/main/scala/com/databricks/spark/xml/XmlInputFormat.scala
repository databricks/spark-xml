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
import java.nio.charset.Charset

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
  /** configuration key for encoding type */
  val ENCODING_KEY: String = "xmlinput.encoding"
}

/**
 * XMLRecordReader class to read through a given xml document to output xml blocks as records
 * as specified by the start tag and end tag
 */
private[xml] class XmlRecordReader extends RecordReader[LongWritable, Text] {
  private var startTag: Array[Byte] = _
  private var currentStartTag: Array[Byte] = _
  private var endTag: Array[Byte] = _
  private var endEmptyTag: Array[Byte] = _
  private var space: Array[Byte] = _
  private var newLineSpace: Seq[Array[Byte]] = _
  private var angleBracket: Array[Byte] = _

  private var currentKey: LongWritable = _
  private var currentValue: Text = _

  private var start: Long = _
  private var end: Long = _
  private var in: InputStream = _
  private var filePosition: Seekable = _
  private var decompressor: Decompressor = _

  private val buffer: DataOutputBuffer = new DataOutputBuffer

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    val fileSplit: FileSplit = split.asInstanceOf[FileSplit]
    val conf: Configuration = context.getConfiguration
    val charset =
      Charset.forName(conf.get(XmlInputFormat.ENCODING_KEY, XmlOptions.DEFAULT_CHARSET))
    startTag = conf.get(XmlInputFormat.START_TAG_KEY).getBytes(charset)
    endTag = conf.get(XmlInputFormat.END_TAG_KEY).getBytes(charset)
    endEmptyTag = "/>".getBytes(charset)
    space = " ".getBytes(charset)
    newLineSpace = Seq("\r\n", "\n", "\r").map(_.getBytes(charset))
    angleBracket = ">".getBytes(charset)
    require(startTag != null, "Start tag cannot be null.")
    require(endTag != null, "End tag cannot be null.")
    require(space != null, "White space cannot be null.")
    require(angleBracket != null, "Angle bracket cannot be null.")
    start = fileSplit.getStart
    end = start + fileSplit.getLength

    // open the file and seek to the start of the split
    val path = fileSplit.getPath
    val fs = path.getFileSystem(conf)
    val fsin = fs.open(fileSplit.getPath)

    val codec = new CompressionCodecFactory(conf).getCodec(path)
    if (null != codec) {
      decompressor = CodecPool.getDecompressor(codec)
      codec match {
        case sc: SplittableCompressionCodec =>
          val cIn = sc.createInputStream(
            fsin,
            decompressor,
            start,
            end,
            SplittableCompressionCodec.READ_MODE.BYBLOCK)
          start = cIn.getAdjustedStart
          end = cIn.getAdjustedEnd
          in = cIn
          filePosition = cIn
        case c: CompressionCodec =>
          if (start != 0) {
            // So we have a split that is only part of a file stored using
            // a Compression codec that cannot be split.
            throw new IOException("Cannot seek in " +
              codec.getClass.getSimpleName + " compressed stream")
          }
          val cIn = c.createInputStream(fsin, decompressor)
          in = cIn
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
    if (readUntilStartElement()) {
      try {
        buffer.write(currentStartTag)
        if (readUntilEndElement()) {
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

  private def readUntilStartElement(): Boolean = {
    currentStartTag = startTag
    var i = 0
    while (true) {
      val b = in.read()
      if (b == -1 || (i == 0 && filePosition.getPos > end)) {
        // End of file or end of split.
        return false
      } else {
        if (b.toByte == startTag(i)) {
          if (i >= startTag.length - 1) {
            // Found start tag.
            return true
          } else {
            // In start tag.
            i += 1
          }
        } else {
          if (i == (startTag.length - angleBracket.length) && checkAttributes(b)) {
            // Found start tag with attributes.
            return true
          } else {
            // Not in start tag.
            i = 0
          }
        }
      }
    }
    // Unreachable.
    false
  }

  private def checkEmptyTag(currentLetter: Int, position: Int): Boolean = {
    if (position >= endEmptyTag.length) false
    else currentLetter == endEmptyTag(position)
  }

  private def readUntilEndElement(): Boolean = {
    var si = 0
    var ei = 0
    var depth = 0

    while (true) {
      val rb = in.read()
      if (rb == -1) {
        // End of file (ignore end of split).
        return false
      } else {
        buffer.write(rb)
        val b = rb.toByte
        if (b == startTag(si) && (b == endTag(ei) || checkEmptyTag(b, ei))) {
          // In start tag or end tag.
          si += 1
          ei += 1
        } else if (b == startTag(si)) {
          if (si >= startTag.length - 1) {
            // Found start tag.
            si = 0
            ei = 0
            depth += 1
          } else {
            // In start tag.
            si += 1
            ei = 0
          }
        } else if (b == endTag(ei) || checkEmptyTag(b, ei)) {
          if ((b == endTag(ei) && ei >= endTag.length - 1) ||
            (checkEmptyTag(b, ei) && ei >= endEmptyTag.length - 1)) {
            if (depth == 0) {
              // Found closing end tag.
              return true
            } else {
              // Found nested end tag.
              si = 0
              ei = 0
              depth -= 1
            }
          } else {
            // In end tag.
            si = 0
            ei += 1
          }
        } else {
          // Not in start tag or end tag.
          si = 0
          ei = 0
        }
      }
    }
    // Unreachable.
    false
  }

  private def checkAttributes(current: Int): Boolean = {
    var len = 0
    var b = current
    while (len < space.length && b == space(len) || newLineSpace.exists(_.apply(len) == b)) {
      len += 1
      if (len >= space.length) {
        currentStartTag = startTag.take(startTag.length - angleBracket.length) ++ space
        return true
      }
      b = in.read
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
        CodecPool.returnDecompressor(decompressor)
        decompressor = null
      }
    }
  }
}
