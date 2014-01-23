/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v2_0.newCompiler

import org.neo4j.cypher.internal.compiler.v2_0.spi.QueryContext
import org.neo4j.graphdb.{Direction, Relationship, Node}
import collection.mutable

trait Operator {
  def open()

  def next(): Boolean

  def close()

  def toList(data: mutable.Map[Int, Any]) = {
    val listBuilder = List.newBuilder[Map[Int, Any]]

    open()
    try {
      while (next()) {
        listBuilder += data.toMap
      }
      listBuilder.result()
    } finally {
      close()
    }
  }
}

case class AllNodesScanOp(qtx: QueryContext, id: Int, data: mutable.Map[Int, Any]) extends Operator {
  private val allNodes: Iterator[Node] = qtx.nodeOps.all

  def open() {}

  def next(): Boolean =
    if (allNodes.hasNext) {
      data(id) = allNodes.next().getId
      true
    } else false

  def close() {}
}

case class LabelScanOp(qtx: QueryContext, id: Int, labelToken: Int, data: mutable.Map[Int, Any]) extends Operator {

  private val labeledNodes: Iterator[Node] = qtx.getNodesByLabel(labelToken)

  def open() {}

  def next(): Boolean =
    if (labeledNodes.hasNext) {
      data(id) = labeledNodes.next().getId
      true
    } else false

  def close() {}

}

case class ExpandOp(qtx: QueryContext, sourceId: Int, destinationId: Int, sourceOp: Operator, data: mutable.Map[Int, Any]) extends Operator {

  var currentRelationships: Iterator[Relationship] = Iterator.empty
  def fromNode: Node = {
    val fromNode = data(sourceId).asInstanceOf[Long]
    qtx.nodeOps.getById(fromNode)
  }

  def open() {
    sourceOp.open()
  }

  def next(): Boolean = {
    while (currentRelationships.isEmpty && sourceOp.next()) {
      currentRelationships = qtx.getRelationshipsFor(fromNode, Direction.OUTGOING, Seq.empty)
    }

    if(currentRelationships.hasNext) {
      val r: Relationship = currentRelationships.next()
      data(destinationId) = r.getOtherNode(fromNode).getId
      true
    } else false
  }

  def close() {}

}
