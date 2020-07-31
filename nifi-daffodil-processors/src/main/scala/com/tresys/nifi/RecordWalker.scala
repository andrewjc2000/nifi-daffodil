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

import java.util.{ArrayList => JArrayList, List => JList}

import org.apache.daffodil.dsom.walker.{AbstractDSOMWalker, ChoiceView, ElementBaseView, RootView, SequenceView, TermView, TypeView}
import org.apache.nifi.serialization.SimpleRecordSchema
import org.apache.nifi.serialization.record.{DataType, RecordField, RecordFieldType, RecordSchema, SchemaIdentifier}
import org.apache.nifi.serialization.record.`type`.RecordDataType
import scala.collection.mutable.ListBuffer

/**
 * Direct subclass of the NIFI RecordField.  NIFI doesn't have "Optional" fields, so
 * eventually this will either be removed from the Schema if an Infoset doesn't have the field,
 * or it will become a regular RecordField if it does.
 * @param recordField an already existing RecordField from which the Name and DataType will be obtained
 */
class OptionalRecordField(recordField: RecordField)
  extends RecordField(recordField.getFieldName, recordField.getDataType) {
  override def equals(obj: Any): Boolean =
    obj match {
      case _: OptionalRecordField => super.equals(obj)
      case _ => false
    }
  override def hashCode(): Int = 31 * super.hashCode() + 1
  override def toString: String = "Optional" + super.toString
}

/**
 * Concrete implementation of the AbstractDSOMWalker abstract class.
 * This class produces a NIFI RecordSchema that is intended to match the original DFDL file.
 *
 * The RecordSchema is built in 3 primary stages:
 * 1) A tree of SchemaNodes is created as the DFDL file is walked; this walk is performed
 * through the various event handlers defined in the parent abstract class.
 * 2) The tree of SchemaNodes undergoes some post-processing, mainly to remove redundant Record wrappers.
 * 3) The tree of SchemaNodes is converted into a RecordSchema; it is walked recursively within this class.
 */
class RecordWalker extends AbstractDSOMWalker {

  // this is the critical data structure for managing the temporary SchemaNodes that are created
  // when the Schema is initially walked.  This will then be converted to the actual RecordSchema.
  private var objectStack: List[RecordSchemaNode] = List()

  // this will contain the final RecordSchema after step 3 as described above is complete.
  private var result: Option[RecordSchema] = None
  def getResult: RecordSchema = result.orNull
  def stringRep: String = if (result.isDefined) result.get.toString else ""

  override def onTermBegin(termElement: TermView): Unit = termElement match {
    case _: SequenceView | _: ChoiceView | _: ElementBaseView =>
      val newNode: RecordSchemaNode = new RecordSchemaNode(termElement)
      // we need add the new node as a new child of whatever is currently at the top of the stack
      objectStack.head.addChild(newNode)
      // however, we also add the node to the stack itself!  We need to be able to add children to it
      // if it is, say, another record or array.
      objectStack = newNode +: objectStack
    case _ =>
  }

  override def onTermEnd(termElement: TermView): Unit = termElement match {
    case _: SequenceView | _: ChoiceView | _: ElementBaseView => objectStack = objectStack.tail
    case _ =>
  }

  override def onTypeBegin(typeElement: TypeView): Unit = {}

  override def onTypeEnd(typeElement: TypeView): Unit = {}

  override def onWalkBegin(root: RootView): Unit = objectStack = List(new RecordSchemaNode(root))

  /**
   * Perform postProcessing; this happens *after* the SchemaNode tree is created but *before* that tree
   * gets converted to a RecordSchema
   */
  private def postProcessing(): Unit = {
    removeExtraRecords(objectStack.head)
  }

  /**
   * Recursively replace any SchemaNodes that are of type record and do not have
   * a name attribute with their children. These usually represent unnecessary wrapper nodes or empty
   * records with no elements.
   *
   * Given a SchemaNode, we copy over all the children to a list, and initialize another empty list.
   * Then, for every Node in the first list, if it is considered "extra",
   * then its *children* are added to the second list.
   * Otherwise, the Node itself is added.
   *
   * Then, we let the first list equal the second and we repeat this process
   * until none of the children are "extra".
   * This is necessary because sometimes we can have towers of extra Nodes that would
   * never get resolved if we just took care of 1 or 2 layers; all must be dealt with at once.
   *
   * Then, the current SchemaNode's children list is replaced with this filtered one,
   * and this method is called on all the items of that list.
   * @param schemaNode the current Node undergoing the algorithm described above
   */
  private def removeExtraRecords(schemaNode: RecordSchemaNode): Unit = {
    schemaNode.recordType match {
      // Only SchemaNodes that are these 3 types should be able to have child nodes.
      case RecordFieldType.RECORD | RecordFieldType.CHOICE | RecordFieldType.ARRAY =>
        if (schemaNode.children.exists(child => isExtraRecord(schemaNode, child))) {
          var currChildren: ListBuffer[RecordSchemaNode] = new ListBuffer[RecordSchemaNode]()
          for (child <- schemaNode.children) currChildren += child
          while (currChildren.exists(child => isExtraRecord(schemaNode, child))) {
            val toCopy: ListBuffer[RecordSchemaNode] = new ListBuffer[RecordSchemaNode]()
            for (child <- currChildren) {
              if (isExtraRecord(schemaNode, child)) {
                for (subChild <- child.children) toCopy += subChild
              } else toCopy += child
            }
            currChildren = toCopy
          }
          schemaNode.children = currChildren.toList
        }
      case _ =>
    }
    // call this helper method on each of this Nodes's children.  There is no infinite recursion because
    // eventually a sub-child will not have any more children.  This of course assumes that we are dealing
    // with a rooted tree (there shouldn't be any cycles), which should be the case.
    for (child <- schemaNode.children) removeExtraRecords(child)
  }

  /**
   * Determines if a Record is "extra"; that is, if it should be replaced with
   * its list of children SchemaNodes within whatever parent SchemaNode it's a part of
   * @param childNode the node to be considered
   * @return true if this node is a Record and has no name, false otherwise
   */
  private def isExtraRecord(parentNode: RecordSchemaNode, childNode: RecordSchemaNode): Boolean = {
    // any no-name nodes with no children are immediately removed
    (childNode.name.isEmpty && childNode.children.isEmpty) || {
      parentNode.recordType match {
        case RecordFieldType.RECORD | RecordFieldType.ARRAY =>
          childNode.recordType match {
            // This removes extra wrapper records around children of records or arrays
            // usually used to remove things like the DFDL complexType, simpleType elements
            case RecordFieldType.RECORD => childNode.name.isEmpty
            case _ => false
          }
        // Currently, all double choices are removed.  This was mainly done to make GroupRefs work
        // for the JPEG Schema, but may not be the correct approach for all cases.
        case RecordFieldType.CHOICE =>
          childNode.recordType match {
            case RecordFieldType.CHOICE => true
            case _ => false
          }
        case _ => false
      }
    }

  }

  override def onWalkEnd(root: RootView): Unit = {
    // After the walk is over, we perform postProcessing and then convert the SchemaNode tree
    // into a RecordSchema.  Also, if we are in dev. mode, we print out the SchemaNode tree
    if (!RecordUtil.PRODUCTION_MODE) println(objectStack.head)
    postProcessing()
    if (!RecordUtil.PRODUCTION_MODE) println(objectStack.head)
    result = Some(schemaNodeToRecordType(objectStack.head).getChildSchema)
  }

  /**
   * Helper method to specifically convert a SchemaNode known to be a Record into a NIFI RecordSchema.
   * Each Child of the SchemaNode becomes a Field of the RecordSchema
   * @param schemaNode the node to convert to a RecordSchema
   * @return a tuple containing the finalized RecordSchema and its corresponding NIFI Record Data Type
   */
  private def schemaNodeToRecordType(schemaNode: RecordSchemaNode): RecordDataType = {
    val fieldList: JList[RecordField] = new JArrayList[RecordField]()
    for (child <- schemaNode.children) fieldList.add(nodeToField(child))
    val recordSchema: SimpleRecordSchema = newRecordSchema(schemaNode, fieldList)
    new RecordDataType(recordSchema)
  }

  private def nodeToField(schemaNode: RecordSchemaNode): RecordField = {
    // by default, if this node doesn't have a name, its data type is used as the field name.
    // This should only ever be the case for anonymous choices.
    val recordField: RecordField = new RecordField(
      schemaNode.name.getOrElse(schemaNode.recordType.getDataType.toString),
      schemaNodeToDataType(schemaNode)
    )
    if (schemaNode.isOptional) new OptionalRecordField(recordField) else recordField
  }

  private def newRecordSchema(schemaNode: RecordSchemaNode,
                              fieldList: JList[RecordField]): SimpleRecordSchema = {
    val newSchema: SimpleRecordSchema = new SimpleRecordSchema(SchemaIdentifier.EMPTY)
    newSchema.setSchemaName(schemaNode.name.getOrElse(""))
    newSchema.setSchemaNamespace(schemaNode.namespace)
    newSchema.setFields(fieldList)
    newSchema
  }

  /**
   * Helper method to convert a SchemaNode known to be a choice into a NIFI Choice data type.
   * This is able to handle a DFDL schema in which either named elements are directly sub-members of
   * DFDL choices or if they are embedded in another element (which corresponds to being in a NIFI Record).
   * In the end, if they are not already inside a NIFI Record, then they are put there.  NIFI Choices cannot
   * have fields, only possible sub-types, so anything that would be a "field" has a wrapper NIFI Record
   * put around it.
   * @param schemaNode the node to convert to a NIFI Choice data type
   * @return a NIFI Choice data type as described above
   */
  private def choiceNodeToChoiceSchema(schemaNode: RecordSchemaNode): DataType = {
    val childList: JList[DataType] = new JArrayList[DataType]()
    for (child <- schemaNode.children) {
      if (child.name.isEmpty) {
        childList.add(schemaNodeToRecordType(child))
      } else {
        // In the case where the child has a name, we embed it in an additional RecordSchema,
        // since we must add some DataType rather than a Field.
        val fieldList: JList[RecordField] = new JArrayList[RecordField]()
        fieldList.add(nodeToField(child))
        val choiceOption: SimpleRecordSchema = newRecordSchema(child, fieldList)
        childList.add(RecordFieldType.RECORD.getRecordDataType(choiceOption))
      }
    }
    RecordFieldType.CHOICE.getChoiceDataType(childList)
  }

  /**
   * Local helper method to appropriately convert a SchemaNode into an appropriate NIFI Record
   * Data Type.  Records and Choices get routed to other helper methods, and Arrays are handled in the method.
   * @param schemaNode the node to convert to a NIFI Record Data Type
   * @return the finalized NIFI Record Data type
   */
  private def schemaNodeToDataType(schemaNode: RecordSchemaNode): DataType = {
    schemaNode.recordType match {
      case RecordFieldType.ARRAY =>
        if (schemaNode.isSimple) {
          RecordFieldType.ARRAY.getArrayDataType(schemaNode.simpleType.get.getDataType)
        } else if (schemaNode.children.nonEmpty) {
          RecordFieldType.ARRAY.getArrayDataType(schemaNodeToRecordType(schemaNode))
        } else {
          throw new IllegalStateException(
            s"Array Node $schemaNode either needs to be simple or have child nodes!"
          )
        }
      case RecordFieldType.RECORD => schemaNodeToRecordType(schemaNode)
      case RecordFieldType.CHOICE => choiceNodeToChoiceSchema(schemaNode)
      case recordType => recordType.getDataType
    }
  }

}
