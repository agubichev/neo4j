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
package org.neo4j.cypher.internal.mutation

import org.neo4j.cypher.internal.commands.expressions.Expression
import org.neo4j.cypher.internal.pipes.QueryState
import org.neo4j.cypher.CypherTypeException
import org.neo4j.cypher.internal.symbols._
import collection.JavaConverters._
import org.neo4j.kernel.impl.core.NodeManager
import org.neo4j.graphdb.{PropertyContainer, Path, Relationship, Node}
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.data.{RelationshipThingie, NodeThingie, Entity}

case class DeleteEntityAction(elementToDelete: Expression)
  extends UpdateAction {
  def exec(context: ExecutionContext, state: QueryState) = {
    elementToDelete(context)(state) match {
      case n: NodeThingie         => delete(n, state)
      case r: RelationshipThingie => delete(r, state)
      case null                   =>

        // TODO: Path should be PathThingie
      case p: Path => p.
        iterator().asScala.
        foreach {
        case n: Node         => delete(NodeThingie(n.getId), state)
        case r: Relationship => delete(RelationshipThingie(r.getId), state)
      }

      case x                      => throw new CypherTypeException("Expression `" + elementToDelete.toString() + "` yielded `" + x.toString + "`. Don't know how to delete that.")
    }

    Iterator(context)
  }

  private def delete(x: Entity, state: QueryState) {
    val nodeManager: NodeManager = state.graphDatabaseAPI.getNodeManager

    x match {
      case n: NodeThingie if !nodeManager.isDeleted(state.query.getNodeById(n.id)) =>
        state.query.nodeOps.delete(n.id)

      case r: RelationshipThingie if !nodeManager.isDeleted(state.query.getRelationshipById(r.id)) =>
        state.query.relationshipOps.delete(r.id)

      case _ => // Entity is already deleted. No need to do anything
    }
  }

  def identifiers: Seq[(String, CypherType)] = Nil

  def rewrite(f: (Expression) => Expression) = DeleteEntityAction(elementToDelete.rewrite(f))

  def children = Seq(elementToDelete)

  def throwIfSymbolsMissing(symbols: SymbolTable) {
    elementToDelete.throwIfSymbolsMissing(symbols)
  }

  def symbolTableDependencies = elementToDelete.symbolTableDependencies
}