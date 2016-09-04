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
  private var space: Array[Byte] = _
  private var angleBracket: Array[Byte] = _

  private var currentKey: LongWritable = _
  private var currentValue: Text = _

  private var start: Long = _
  private var end: Long = _
  private var in: InputStream = _
  private var filePosition: Seekable = _
  private var decompressor: Decompressor = _
  private var compressRatio: Double = _
  
  private val buffer: DataOutputBuffer = new DataOutputBuffer

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    val fileSplit: FileSplit = split.asInstanceOf[FileSplit]
    val conf: Configuration = {
      // Use reflection to get the Configuration. This is necessary because TaskAttemptContext is
      // a class in Hadoop 1.x and an interface in Hadoop 2.x.
      val method = context.getClass.getMethod("getConfiguration")
      method.invoke(context).asInstanceOf[Configuration]
    }
    val charset =
      Charset.forName(conf.get(XmlInputFormat.ENCODING_KEY, XmlOptions.DEFAULT_CHARSET))
    startTag = conf.get(XmlInputFormat.START_TAG_KEY).getBytes(charset)
    endTag = conf.get(XmlInputFormat.END_TAG_KEY).getBytes(charset)
    space = " ".getBytes(charset)
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
      // Use reflection to get the splittable compression codec and stream. This is necessary
      // because SplittableCompressionCodec does not exist in Hadoop 1.0.x.
      def isSplitCompressionCodec(obj: Any) = {
        val splittableClassName = "org.apache.hadoop.io.compress.SplittableCompressionCodec"
        obj.getClass.getInterfaces.map(_.getName).contains(splittableClassName)
      }
      // Here I made separate variables to avoid to try to find SplitCompressionInputStream at
      // runtime.
      val (inputStream, seekable) = codec match {
        case c: CompressionCodec if isSplitCompressionCodec(c) =>
          // At Hadoop 1.0.x, this case would not be executed.
          val cIn = {
            val sc = c.asInstanceOf[SplittableCompressionCodec]
            sc.createInputStream(fsin, decompressor, start,
              end, SplittableCompressionCodec.READ_MODE.BYBLOCK)
          }
          compressRatio = cIn.getAdjustedEnd / end.toDouble
          start = cIn.getAdjustedStart
          end = cIn.getAdjustedEnd
          (cIn, cIn)
        case c: CompressionCodec =>
          if (start != 0) {
            // So we have a split that is only part of a file stored using
            // a Compression codec that cannot be split.
            throw new IOException("Cannot seek in " +
              codec.getClass.getSimpleName + " compressed stream")
          }
          val cIn = c.createInputStream(fsin, decompressor)
          (cIn, fsin)
      }
     in = inputStream
     filePosition = seekable
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
    //Stop if reaching the end of the current input split
    if(in.isInstanceOf[SplitCompressionInputStream] && filePosition.getPos * compressRatio > end) false
    else if(!in.isInstanceOf[CompressionInputStream] && filePosition.getPos > end) false
    else 
      if (readUntilMatch(startTag, withinBlock = false)) {
      try {
        buffer.write(currentStartTag)
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
    while(true) {
      val b: Int = in.read
      if (b == -1) {
        currentStartTag = startTag
        return false
      }
      if (withinBlock) {
        buffer.write(b)
      }
      if (b == mat(i)) {
        i += 1
        if (i >= mat.length) {
          currentStartTag = startTag
          return true
        }
      }
      else {
        // The start tag might have attributes. In this case, we decide it by the space after tag
        if (i == (mat.length - angleBracket.length) && !withinBlock) {
          if (checkAttributes(b)){
            return true
          }
        }
        i = 0
      }
    }
    false
  }

  private def checkAttributes(current: Int): Boolean = {
    var len = 0
    var b = current
    while(len < space.length && b == space(len)) {
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
