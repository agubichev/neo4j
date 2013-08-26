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

import org.neo4j.graphdb._
import org.neo4j.kernel.impl.api.index.IndexDescriptor
import org.neo4j.cypher.internal.data.{Entity, RelationshipThingie, NodeThingie}

/*
 * Developer note: This is an attempt at an internal graph database API, which defines a clean cut between
 * two layers, the query engine layer and, for lack of a better name, the core database layer.
 *
 * Building the query engine layer on top of an internal layer means we can move much faster, not
 * having to worry about deprecations and so on. It is also acceptable if this layer is a bit clunkier, in this
 * case we are, for instance, not exposing any node or relationship objects, but provide direct methods for manipulating
 * them by ids instead.
 *
 * The driver for this was clarifying who is responsible for ensuring query isolation. By exposing a query concept in
 * the core layer, we can move that responsibility outside of the scope of cypher.
 */
trait QueryContext extends TokenContext {

  def nodeOps: Operations[NodeThingie]

  def relationshipOps: Operations[RelationshipThingie]

  def createNode(): NodeThingie

  def createRelationship(start: Long, end: Long, relType: String): RelationshipThingie

  def getRelationshipsFor(node: Long, dir: Direction, types: Seq[String]): Iterator[RelationshipThingie]

  def getOrCreateLabelId(labelName: String): Long

  def getLabelsForNode(node: Long): Iterator[Long]

  def isLabelSetOnNode(label: Long, node: Long): Boolean = getLabelsForNode(node).toIterator.contains(label)

  def setLabelsOnNode(node: Long, labelIds: Iterator[Long]): Int

  def removeLabelsFromNode(node: Long, labelIds: Iterator[Long]): Int

  def getOrCreatePropertyKeyId(propertyKey: String): Long

  def addIndexRule(labelIds: Long, propertyKeyId: Long)

  def dropIndexRule(labelIds: Long, propertyKeyId: Long)

  def close(success: Boolean)

  def exactIndexSearch(index: IndexDescriptor, value: Any): Iterator[NodeThingie]

  def getNodesByLabel(id: Long): Iterator[NodeThingie]

  def upgradeToLockingQueryContext: LockingQueryContext = upgrade(this)

  def upgrade(context: QueryContext): LockingQueryContext

  def getOrCreateFromSchemaState[K, V](key: K, creator: => V): V

  def schemaStateContains(key: String): Boolean

  def createUniqueConstraint(labelId:Long, propertyKeyId:Long)

  def dropUniqueConstraint(labelId:Long, propertyKeyId:Long)

  /**
   * This should not be used. We'll remove sooner (or later). Don't do it.
   */
  def withAnyOpenQueryContext[T](work: (QueryContext) => T): T

  def getOtherNodeFor(relationship:Long, node:Long):NodeThingie

  def getNodeById(id:Long):Node
  def getRelationshipById(id:Long):Relationship
  def getRelationshipType(id:Long):String
  def getStartNode(relationship:Long):NodeThingie
  def getEndNode(relationship:Long):NodeThingie
}

trait LockingQueryContext extends QueryContext {
  def releaseLocks()
}

trait Operations[T <: Entity] {
  def delete(id: Long)

  def setProperty(id: Long, propertyKeyId: Long, value: Any)

  def removeProperty(id: Long, propertyKeyId: Long)

  def getProperty(id: Long, propertyKeyId: Long): Any

  def hasProperty(id: Long, propertyKeyId: Long): Boolean

  def propertyKeyIds(id: Long): Iterator[Long]

  def propertyKeys(id: Long): Iterator[String]

  def indexGet(name: String, key: String, value: Any): Iterator[T]

  def indexQuery(name: String, query: Any): Iterator[T]

  def all : Iterator[T]
}
