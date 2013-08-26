/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.spi

import org.neo4j.graphdb.{PropertyContainer, Relationship, Direction, Node}
import org.neo4j.kernel.impl.api.index.IndexDescriptor
import org.neo4j.cypher.internal.data.{Entity, RelationshipThingie, NodeThingie}


trait Locker {
  def acquireLock(p: Entity)

  def releaseAllLocks()
}

class RepeatableReadQueryContext(inner: QueryContext, locker: Locker) extends DelegatingQueryContext(inner) with LockingQueryContext {

  override def getRelationshipsFor(node: Long, dir: Direction, types: Seq[String]): Iterator[RelationshipThingie] = {
    locker.acquireLock(NodeThingie(node))
    lockAll(inner.getRelationshipsFor(node, dir, types))
  }

  override def getLabelsForNode(node: Long): Iterator[Long] = {
    lockNode(node)
    inner.getLabelsForNode(node)
  }

  override def isLabelSetOnNode(label: Long, node: Long): Boolean = {
    lockNode(node)
    inner.isLabelSetOnNode(label, node)
  }

  override def exactIndexSearch(index: IndexDescriptor, value: Any): Iterator[NodeThingie] =
    lockAll(inner.exactIndexSearch(index, value))

  override def getRelationshipType(id: Long): String = {
    val r = RelationshipThingie(id)
    locker.acquireLock(r)
    inner.getRelationshipType(id)
  }

  override def getNodesByLabel(id: Long): Iterator[NodeThingie] = lockAll(inner.getNodesByLabel(id))

  override def getStartNode(relationship: Long): NodeThingie = {
    val r = RelationshipThingie(relationship)
    locker.acquireLock(r)
    inner.getStartNode(relationship)
  }

  override def getEndNode(relationship: Long): NodeThingie = {
    val r = RelationshipThingie(relationship)
    locker.acquireLock(r)
    inner.getEndNode(relationship)
  }


  val nodeOpsValue = new RepeatableReadOperations[NodeThingie](inner.nodeOps) {
    def getEntity(id: Long): Entity = NodeThingie(id)
  }

  val relationshipOpsValue = new RepeatableReadOperations[RelationshipThingie](inner.relationshipOps) {
    def getEntity(id: Long): Entity = RelationshipThingie(id)
  }

  override def nodeOps = nodeOpsValue

  override def relationshipOps = relationshipOpsValue

  def releaseLocks() {
    locker.releaseAllLocks()
  }

  abstract class RepeatableReadOperations[T <: Entity](inner: Operations[T]) extends DelegatingOperations[T](inner) {

    def getEntity(id:Long):Entity

    override def getProperty(id: Long, propertyKeyId: Long) = {
      locker.acquireLock(getEntity(id))
      inner.getProperty(id, propertyKeyId)
    }

    override def hasProperty(id: Long, propertyKeyId: Long) = {
      locker.acquireLock(getEntity(id))
      inner.hasProperty(id, propertyKeyId)
    }

    override def propertyKeys(id: Long) = {
      locker.acquireLock(getEntity(id))
      inner.propertyKeys(id)
    }

    override def indexGet(name: String, key: String, value: Any): Iterator[T] = lockAll(inner.indexGet(name, key, value))

    override def indexQuery(name: String, query: Any): Iterator[T] = lockAll(inner.indexQuery(name, query))
  }

  private def lockNode(id: Long) {
    locker.acquireLock(NodeThingie(id))
  }

  private def lockAll[T <: Entity](iter: Iterator[T]): Iterator[T] = iter.map {
    item =>
      locker.acquireLock(item)
      item
  }
}



