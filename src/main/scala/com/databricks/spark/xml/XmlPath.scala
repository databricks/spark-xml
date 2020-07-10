/*
 * Copyright 2020 Onedot AG
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

case class XmlPath(elements: Seq[String]) {
  val leafName = elements.lastOption.getOrElse("")

  def isRoot: Boolean = elements == Nil

  def isChildOf(parent: XmlPath): Boolean = {
    val matching = elements.zip(parent.elements).view
    parent != this && matching.length == parent.elements.length && matching.forall { case (left, right) => left == right }
  }

  def renameLeaf(newName: String): XmlPath = if (isRoot) {
    throw new IllegalStateException(s"Cannot rename root element")
  } else {
    XmlPath(elements.dropRight(1) :+ newName)
  }

  def child(childName: String): XmlPath = XmlPath(elements :+ childName)

  def parent: XmlPath = if (isRoot) {
    throw new IllegalStateException(s"Root element does not have parents")
  } else {
    XmlPath(elements.dropRight(1))
  }

  override def toString: String = elements.mkString("/")
}

object XmlPath {
  val root = XmlPath(Seq.empty)
}