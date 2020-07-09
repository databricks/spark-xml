/**
 * Copyright 2020 Onedot AG.
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
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