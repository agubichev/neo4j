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
package org.neo4j.cypher.internal.spi.gdsimpl

import org.neo4j.cypher.internal.spi._
import org.neo4j.graphdb._
import org.neo4j.kernel.{ThreadToStatementContextBridge, GraphDatabaseAPI}
import org.neo4j.kernel.api._
import collection.JavaConverters._
import org.neo4j.graphdb.DynamicRelationshipType.withName
import org.neo4j.cypher._
import org.neo4j.tooling.GlobalGraphOperations
import collection.mutable
import org.neo4j.kernel.impl.api.index.IndexDescriptor
import org.neo4j.helpers.collection.IteratorUtil.singleOrNull
import org.neo4j.kernel.api.operations.StatementTokenNameLookup
import org.neo4j.kernel.api.exceptions.KernelException
import org.neo4j.kernel.api.exceptions.schema.{SchemaKernelException, DropIndexFailureException}
import org.neo4j.kernel.api.operations.StatementState
import org.neo4j.kernel.impl.api.PrimitiveLongIterator
import scala.collection.Iterator
import org.neo4j.cypher.internal.helpers.JavaConversionSupport
import org.neo4j.cypher.internal.helpers.JavaConversionSupport.mapToScala
import org.neo4j.cypher.internal.data.{Entity, RelationshipThingie, NodeThingie}

class TransactionBoundQueryContext(graph: GraphDatabaseAPI, tx: Transaction,
                                   ctx: StatementOperationParts, theState: StatementState)
  extends TransactionBoundTokenContext(ctx.keyReadOperations, theState) with QueryContext {

  private var open = true

  def setLabelsOnNode(node: Long, labelIds: Iterator[Long]): Int = labelIds.foldLeft(0) {
    case (count, labelId) =>
      if (ctx.entityWriteOperations.nodeAddLabel(theState, node, labelId))
        count + 1
      else
        count
  }

  def close(success: Boolean) {
    try {
      theState.close()

      if (success)
        tx.success()
      else
        tx.failure()
      tx.finish()
    }
    finally {
      open = false
    }
  }

  def withAnyOpenQueryContext[T](work: (QueryContext) => T): T = {
    if (open) {
      work(this)
    }
    else {
      val tx = graph.beginTx()
      try {
        val bridge   = graph.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge])
        val stmCtx   = bridge.getCtxForReading
        val state    = bridge.statementForReading
        val result   = try {
          work(new TransactionBoundQueryContext(graph, tx, stmCtx, state))
        }
        finally {
          state.close()
        }
        tx.success()
        result
      }
      finally {
        tx.finish()
      }
    }
  }

  def createNode(): NodeThingie = {
    // TODO: We should just get an id, not a node Object
    val n = graph.createNode()
    NodeThingie(n.getId)
  }

  def createRelationship(start: Long, end: Long, relType: String): RelationshipThingie = {
    // TODO: StatementOperations should expose a way to create relationships without needing Node objects
    val s = getNodeById(start)
    val e = getNodeById(end)
    val r = s.createRelationshipTo(e, withName(relType))
    RelationshipThingie(r.getId)
  }

  def getLabelsForNode(node: Long) =
    JavaConversionSupport.asScala( ctx.entityReadOperations.nodeGetLabels(theState, node) )

  override def isLabelSetOnNode(label: Long, node: Long) =
    ctx.entityReadOperations.nodeHasLabel(theState, node, label)

  def getOrCreateLabelId(labelName: String) =
    ctx.keyWriteOperations.labelGetOrCreateForName(theState, labelName)


  def getRelationshipsFor(nodeId: Long, dir: Direction, types: Seq[String]): Iterator[RelationshipThingie] = {
    // TODO: We should be able to get the relationship ids, and not get the full objects
    val node = getNodeById(nodeId)
    val realRelationships: Iterator[Relationship] = types match {
      case Seq() => node.getRelationships(dir).iterator().asScala
      case _     => node.getRelationships(dir, types.map(withName): _*).iterator().asScala
    }

    realRelationships.map(r => RelationshipThingie(r.getId))
  }

  def getTransaction = tx

  def exactIndexSearch(index: IndexDescriptor, value: Any) =
    mapToScala(ctx.entityReadOperations.nodesGetFromIndexLookup(theState, index, value))(NodeThingie)

  val nodeOps = new NodeOperations

  val relationshipOps = new RelationshipOperations

  def removeLabelsFromNode(node: Long, labelIds: Iterator[Long]): Int = labelIds.foldLeft(0) {
    case (count, labelId) =>
      if (ctx.entityWriteOperations.nodeRemoveLabel(theState, node, labelId))
        count + 1
      else
        count
  }

  def getNodesByLabel(id: Long): Iterator[NodeThingie] =
    mapToScala(ctx.entityReadOperations.nodesGetForLabel(theState, id))(NodeThingie)

  class NodeOperations extends BaseOperations[NodeThingie] {
    def delete(id: Long) {
      ctx.entityWriteOperations.nodeDelete(theState, id)
    }

    def propertyKeyIds(id: Long): Iterator[Long] =
      primitiveLongIteratorToScalaIterator(
        ctx.entityReadOperations.nodeGetPropertyKeys(theState, id)).map(_.longValue())

    def getProperty(id: Long, propertyKeyId: Long): Any = {
      ctx.entityReadOperations.nodeGetProperty(theState, id, propertyKeyId).value(null)
    }

    def hasProperty(id: Long, propertyKey: Long) =
      ctx.entityReadOperations.nodeHasProperty(theState, id, propertyKey)

    def removeProperty(id: Long, propertyKeyId: Long) {
      ctx.entityWriteOperations.nodeRemoveProperty(theState, id, propertyKeyId)
    }

    def setProperty(id: Long, propertyKeyId: Long, value: Any) {
      ctx.entityWriteOperations
        .nodeSetProperty(theState, id, properties.Property.property(propertyKeyId, value))
    }


    def getById(id: Long) = try {
      graph.getNodeById(id)
    } catch {
      case e: NotFoundException => throw new EntityNotFoundException(s"Node with id $id", e)
      case e: RuntimeException  => throw e
    }

    def all: Iterator[NodeThingie] =
      GlobalGraphOperations.
        at(graph).
        getAllNodes.iterator().asScala.
        map(n => NodeThingie(n.getId))

    def indexGet(name: String, key: String, value: Any): Iterator[NodeThingie] =
    //TODO: We should return id's from the index, not Node objects
      graph.
        index.
        forNodes(name).
        get(key, value).iterator().asScala.
        map(n => NodeThingie(n.getId))

    def indexQuery(name: String, query: Any): Iterator[NodeThingie] =
      graph.
        index.
        forNodes(name).
        query(query).iterator().asScala.
        map(n => NodeThingie(n.getId))

    def propertyKeys(id: Long): Iterator[String] =
      ctx.
        entityReadOperations().
        nodeGetAllProperties(theState, id).asScala.
        map(p => ctx.keyReadOperations().propertyKeyGetName(theState, p.propertyKeyId()))
  }

  class RelationshipOperations extends BaseOperations[RelationshipThingie] {
    def delete(id: Long) {
      ctx.entityWriteOperations.relationshipDelete(theState, id)
    }

    def propertyKeyIds(id: Long): Iterator[Long] =
      primitiveLongIteratorToScalaIterator(
        ctx.entityReadOperations.relationshipGetPropertyKeys(theState, id)).map(_.longValue())

    def getProperty(id: Long, propertyKeyId: Long): Any =
      ctx.entityReadOperations.relationshipGetProperty(theState, id, propertyKeyId).value(null)

    def hasProperty(id: Long, propertyKey: Long) =
      ctx.entityReadOperations.relationshipHasProperty(theState, id, propertyKey)

    def removeProperty(id: Long, propertyKeyId: Long) {
      ctx.entityWriteOperations.relationshipRemoveProperty(theState, id, propertyKeyId)
    }

    def setProperty(id: Long, propertyKeyId: Long, value: Any) {
      ctx.entityWriteOperations
        .relationshipSetProperty(theState, id, properties.Property.property(propertyKeyId, value))
    }

    def getById(id: Long) = graph.getRelationshipById(id)

    // TODO: Stupid to have to create the Relationship objects in the first place
    def all: Iterator[RelationshipThingie] =
      GlobalGraphOperations.
        at(graph).
        getAllRelationships.iterator().asScala.
        map(r => RelationshipThingie(r.getId))

    // TODO: Stupid to have to create the Relationship objects in the first place
    def indexGet(name: String, key: String, value: Any): Iterator[RelationshipThingie] =
      graph.
        index.
        forRelationships(name).
        get(key, value).iterator().asScala.
        map(r => RelationshipThingie(r.getId))

    // TODO: Stupid to have to create the Relationship objects in the first place
    def indexQuery(name: String, query: Any): Iterator[RelationshipThingie] =
      graph.
        index.
        forRelationships(name).
        query(query).iterator().asScala.
        map(r => RelationshipThingie(r.getId))

    def propertyKeys(id: Long): Iterator[String] =
      ctx.
        entityReadOperations().
        relationshipGetAllProperties(theState, id).asScala.
        map(p => ctx.keyReadOperations().propertyKeyGetName(theState, p.propertyKeyId()))
  }

  def getOrCreatePropertyKeyId(propertyKey: String) =
    ctx.keyWriteOperations.propertyKeyGetOrCreateForName(theState, propertyKey)

  def addIndexRule(labelIds: Long, propertyKeyId: Long) {
    try {
      ctx.schemaWriteOperations.indexCreate(theState, labelIds, propertyKeyId)
    } catch {
      case e: SchemaKernelException =>
        val labelName = getLabelName(labelIds)
        val propName = ctx.keyReadOperations.propertyKeyGetName(theState, propertyKeyId)
        throw new IndexAlreadyDefinedException(labelName, propName, e)
    }
  }

  def dropIndexRule(labelId: Long, propertyKeyId: Long) {
    try {
      ctx.schemaWriteOperations.indexDrop(theState, new IndexDescriptor(labelId, propertyKeyId))
    } catch {
      case e: DropIndexFailureException =>
        throw new CouldNotDropIndexException(
          e.getUserMessage(new StatementTokenNameLookup(theState, ctx.keyReadOperations)), e)
    }
  }

  def upgrade(context: QueryContext): LockingQueryContext = new RepeatableReadQueryContext(context, new Locker {
    private val locks = new mutable.ListBuffer[Lock]

    def releaseAllLocks() {
      locks.foreach(_.release())
    }

    def acquireLock(e: Entity) {
      val p = e match {
        case n: NodeThingie         => getNodeById(n.id)
        case r: RelationshipThingie => getRelationshipById(r.id)
      }
      locks += tx.acquireWriteLock(p)
    }
  })

  abstract class BaseOperations[T <: Entity] extends Operations[T] {
    protected def primitiveLongIteratorToScalaIterator(primitiveIterator: PrimitiveLongIterator): Iterator[Long] =
      new Iterator[Long] {
        def hasNext: Boolean = primitiveIterator.hasNext

        def next(): Long = primitiveIterator.next
      }
  }

  def getOrCreateFromSchemaState[K, V](key: K, creator: => V) = {
    val javaCreator = new org.neo4j.helpers.Function[K, V]() {
      def apply(key: K) = creator
    }
    ctx.schemaStateOperations.schemaStateGetOrCreate(theState, key, javaCreator)
  }

  def schemaStateContains(key: String) = ctx.schemaStateOperations.schemaStateContains(theState, key)

  def createUniqueConstraint(labelId: Long, propertyKeyId: Long) {
    try {
      ctx.schemaWriteOperations.uniquenessConstraintCreate(theState, labelId, propertyKeyId)
    } catch {
        case e: KernelException =>
          throw new CouldNotCreateConstraintException(
            e.getUserMessage(new StatementTokenNameLookup(theState, ctx.keyReadOperations)), e)
    }
  }

  def dropUniqueConstraint(labelId: Long, propertyKeyId: Long) {
    val constraint = singleOrNull(ctx.schemaReadOperations
                                     .constraintsGetForLabelAndPropertyKey(theState, labelId, propertyKeyId))

    if (constraint == null) {
      throw new MissingConstraintException()
    }

    ctx.schemaWriteOperations.constraintDrop(theState, constraint)
  }

  def getRelationshipById(id: Long): Relationship = graph.getRelationshipById(id)

  def getNodeById(id: Long): Node = graph.getNodeById(id)

  def getOtherNodeFor(relationship: Long, node: Long): NodeThingie = {
    //TODO: Should this data be stored in the RelationshipThingie or should we ask the ctx for it?
    val nodeId = getRelationshipById(relationship).getOtherNode(getNodeById(node)).getId
    NodeThingie(nodeId)
  }

  //TODO: We should be able to do this without having the relationship object
  def getRelationshipType(id: Long): String = getRelationshipById(id).getType.name()

  //TODO: We should be able to do this without having the relationship object
  def getStartNode(relationship: Long): NodeThingie = {
    val realRel: Relationship = getRelationshipById(relationship)
    NodeThingie(realRel.getStartNode.getId)
  }

  //TODO: We should be able to do this without having the relationship object
  def getEndNode(relationship: Long): NodeThingie = {
    val realRel: Relationship = getRelationshipById(relationship)
    NodeThingie(realRel.getEndNode.getId)
  }
}