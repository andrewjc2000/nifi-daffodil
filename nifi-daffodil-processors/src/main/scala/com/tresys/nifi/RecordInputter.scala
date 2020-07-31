/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tresys.nifi

import java.util.{Iterator => JIterator}

import org.apache.daffodil.dpath.NodeInfo
import org.apache.daffodil.infoset.InfosetInputterEventType
import org.apache.daffodil.japi.infoset.InfosetInputter
import org.apache.daffodil.util.MaybeBoolean
import org.apache.daffodil.util.MStackOf
import org.apache.nifi.logging.ComponentLog

/**
 * This class is largely based off of the JDOMInfosetInputter, since the notion
 * of a "Node" in a JDOM Document, as handled in that class, is general enough to be
 * applied here for InfosetNodes
 */
class RecordInputter(val rootNode: InfosetNode, logger: ComponentLog)
  extends InfosetInputter {

  private val nodeStack: MStackOf[(InfosetNode, JIterator[InfosetNode])] = {
    val newStack = new MStackOf[(InfosetNode, JIterator[InfosetNode])]
    val iterator = rootNode.iterator
    if (!iterator.hasNext) {
      throw new IllegalStateException("Root InfosetNode does not contain a root element")
    }
    newStack.push((null, iterator))
    newStack
  }

  var doStartEvent = true

  override def getEventType(): InfosetInputterEventType = {
    import InfosetInputterEventType._
    if (nodeStack.top._1 == null) {
      if (doStartEvent) StartDocument else EndDocument
    } else {
      if (doStartEvent) StartElement else EndElement
    }
  }

  private def nullableString(str: String): String = Option(str).fold("")(s => s)

  override def getLocalName(): String = nodeStack.top._1.getName

  override def getSimpleText(primType: NodeInfo.Kind): String = nullableString(nodeStack.top._1.getValue)

  override def isNilled(): MaybeBoolean = MaybeBoolean.Nope

  override def hasNext(): Boolean = !(nodeStack.top._1 == null && !doStartEvent)

  private def tryDescend(): Boolean = {
    if (nodeStack.top._2.hasNext) {
      val childNode: InfosetNode = nodeStack.top._2.next
      nodeStack.push((childNode, childNode.iterator))
      true
    } else false
  }

  private def stackToString(): String = {
    nodeStack.toList.asInstanceOf[List[(InfosetNode, JIterator[InfosetNode])]]
                    .map(pair => if (pair._1 == null) "" else nullableString(pair._1.getName)).mkString(", ")
  }

  override def next(): Unit = {
    if (hasNext()) {
      RecordUtil.log(logger, s"Current stack: ${stackToString()}")
      RecordUtil.log(logger, s"Superclass stack: ${super.toString}")
      if (tryDescend()) {
        doStartEvent = true
      } else {
        if (doStartEvent) {
          doStartEvent = false
        } else {
          nodeStack.pop
          if (tryDescend()) {
            doStartEvent = true
          }
        }
      }
      RecordUtil.log(logger, s"Modified stack: ${stackToString()}")
      RecordUtil.log(logger, s"Superclass stack: ${super.toString}")
    }
  }

  override def getNamespaceURI(): String = null

  override val supportsNamespaces: Boolean = false

  override def fini: Unit = nodeStack.clear()

}
