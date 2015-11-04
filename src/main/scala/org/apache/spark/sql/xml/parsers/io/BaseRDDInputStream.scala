package org.apache.spark.sql.xml.parsers.io

import java.io.InputStream

import org.apache.spark.rdd.RDD


/**
 * Wraps a base RDD in order to use this as [[InputStream]].
 */
private[sql] class BaseRDDInputStream(xml: RDD[String]) extends InputStream {

  // !! HACK ALERT !!
  //
  // The parsing process actually happens in driver side. So executor node would read and
  // driver node would try to parse the stream. This is reason why Spark supports
  // JSON only formatted by line by line.
  //
  // Currently I could not come up a great trick to process XML documments fomatted without
  // any restrictions.
  //
  // Maybe this can be done by parsing some of possible documents for each partition, then
  // collecting some unparsed docummnets and then, parsing them in driver side.

  var currentRecordBuffer: Option[Array[Byte]] = None
  var pos: Int = 0
  lazy val iter = xml.collect.iterator

  private def checkRead: Boolean = {
    val currentLength = currentRecordBuffer.getOrElse(new Array[Byte](0)).length
    val isCurrentRecordBufferRemaining = pos < currentLength
    if (isCurrentRecordBufferRemaining) {
      true
    } else{
      val isRecordRemaining = iter.hasNext
      currentRecordBuffer = if (isRecordRemaining) {
        pos = 0
        Some(iter.next.getBytes)
      } else {
        None
      }
      isRecordRemaining
    }
  }

  override def read(): Int = {
    if (checkRead) {
      val ret = currentRecordBuffer.map(_(pos))
      pos += 1
      ret.map(_.toInt).getOrElse(-1)
    } else {
      -1
    }
  }
}