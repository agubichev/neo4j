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
import scala.collection.mutable
import org.neo4j.kernel.api.Statement
import org.neo4j.kernel.impl.util.PrimitiveLongIterator
import org.neo4j.helpers.collection.IteratorUtil

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

case class StatementContext(statement: Statement) {
  def read = statement.readOperations()
}

case class AllNodesScanOp(stx: StatementContext, id: Int, data: mutable.Map[Int, Any]) extends Operator {
  private val allNodes: Iterator[Node] = ???

  def open() {}

  def next(): Boolean =
    if (allNodes.hasNext) {
      data(id) = allNodes.next().getId
      true
    } else false

  def close() {}
}

case class LabelScanOp(stx: StatementContext, id: Int, labelToken: Int, data: mutable.Map[Int, Any]) extends Operator {

  private val labeledNodes: PrimitiveLongIterator = stx.read.nodesGetForLabel(labelToken)

  def open() {}

  def next(): Boolean =
    if (labeledNodes.hasNext) {
      data(id) = labeledNodes.next()
      true
    } else false

  def close() {}

}

case class ExpandOp(ctx: QueryContext, sourceId: Int, destinationId: Int, sourceOp: Operator, data: mutable.Map[Int, Any]) extends Operator {

  var currentRelationships: Iterator[Relationship] = Iterator.empty

  def open() {
    sourceOp.open()
  }

  def next(): Boolean = {
    while (!currentRelationships.hasNext && sourceOp.next()) {
      val fromNode: Long = data(sourceId).asInstanceOf[Long]
      currentRelationships = ctx.getRelationshipsFor(ctx.nodeOps.getById(fromNode), Direction.OUTGOING, Seq.empty)
    }

    if (currentRelationships.hasNext) {
      val r: Relationship = currentRelationships.next()
      data(destinationId) = r.getEndNode().getId()
      true
    } else false
  }

  def close() {}

}

case class HashJoinOp(stx: StatementContext, joinKeyId: Int, lhsExtractIds: Seq[Int], rhsExtractIds: Seq[Int], lhs: Operator, rhs: Operator, data: mutable.Map[Int, Any]) extends Operator {
  val map = new mutable.HashMap[Long, mutable.Set[Seq[Long]]] with mutable.MultiMap[Long, Seq[Long]]
  var bucket: Seq[Seq[Long]] = Seq.empty
  var bucketPos: Int = 0

  def open() {
    lhs.open()
    rhs.open()
  }

  def next(): Boolean = {
    while (lhs.next()) {
      val join = data(joinKeyId)
      map.addBinding(join.asInstanceOf[Long], lhsExtractIds.map(data(_).asInstanceOf[Long]))
    }

    while (bucketPos >= bucket.size) {
      if (!rhs.next()) {
        return false
      }
      val join = data(joinKeyId)
      bucket = map.getOrElse(join.asInstanceOf[Long], Set.empty).toSeq
      bucketPos = 0
    }

    val lhsValues = bucket(bucketPos)
    bucketPos += 1
    lhsExtractIds.zipWithIndex.foreach { case (index: Int, targetId: Int) =>
      data(targetId) = lhsValues(index)
    }
    true
  }

  def close() {
    lhs.close()
    rhs.close()
  }

}
