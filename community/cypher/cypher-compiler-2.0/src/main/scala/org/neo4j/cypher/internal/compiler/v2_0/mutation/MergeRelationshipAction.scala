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
package org.neo4j.cypher.internal.compiler.v2_0.mutation

import org.neo4j.cypher.internal.compiler.v2_0.commands.{AstNode, Predicate}
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_0.symbols.{RelationshipType, SymbolTable, CypherType}
import org.neo4j.cypher.internal.compiler.v2_0.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_0.pipes.QueryState
import org.neo4j.cypher.internal.helpers.CastSupport
import org.neo4j.graphdb.{Relationship, Direction, Node}
import org.neo4j.cypher.CypherTypeException

case class MergeRelationshipAction(startNodeIdentifier: String,
                                   endNodeIdentifier: String,
                                   identifier: String,
                                   relType: String,
                                   expectations: Seq[Predicate],
                                   onCreate: Seq[UpdateAction],
                                   onMatch: Seq[UpdateAction]) extends UpdateAction {
  def exec(context: ExecutionContext, state: QueryState): Iterator[ExecutionContext] = {
    val startNode = CastSupport.castOrFail[Node](context(startNodeIdentifier))
    val endNode = CastSupport.castOrFail[Node](context(endNodeIdentifier))
    val foundRelationships: Iterator[ExecutionContext] = findRelationships(context, startNode, endNode)(state)

    if (foundRelationships.isEmpty) {
      val rel = state.query.createRelationship(start = startNode, end = endNode, relType = relType)

      val newContext = context += (identifier -> rel)
      onCreate.foreach(action => action.exec(newContext, state))

      Iterator(newContext)
    } else {
      foundRelationships.map {
        nextContext =>
          onMatch.foreach(_.exec(nextContext, state))
          nextContext
      }
    }
  }

  def findRelationships(context: ExecutionContext, startNode: Node, endNode: Node)(implicit state: QueryState): Iterator[ExecutionContext] = {
    val startIter: Iterator[Relationship] = state.query.getRelationshipsFor(startNode, Direction.OUTGOING, Seq(relType))

    def isConnectedToEndnode = (r: Relationship) => r.getEndNode == endNode
    def insertIntoExecContext = (r: Relationship) => context.newWith(identifier -> r)
    def isOkWithExpectations = (ctx: ExecutionContext) => expectations.forall(predicate => predicate.isMatch(ctx))

    startIter.
    filter(isConnectedToEndnode).
    map(insertIntoExecContext).
    filter(isOkWithExpectations)
  }

  def throwIfSymbolsMissing(in: SymbolTable): Unit = {

    if (in.keys.contains(identifier))
      throw new CypherTypeException(identifier + " already defined.")

    val symbols = in.add(identifier, RelationshipType())

    expectations.foreach(_.throwIfSymbolsMissing(symbols))
    onCreate.foreach(_.throwIfSymbolsMissing(symbols))
    onMatch.foreach(_.throwIfSymbolsMissing(symbols))
  }

  def identifiers: Seq[(String, CypherType)] = Seq(identifier -> RelationshipType())

  def rewrite(f: (Expression) => Expression): UpdateAction = MergeRelationshipAction(
    startNodeIdentifier = startNodeIdentifier,
    endNodeIdentifier = endNodeIdentifier,
    identifier = identifier,
    relType = relType,
    expectations = expectations.map(_.rewrite(f)),
    onCreate = onCreate.map(_.rewrite(f)),
    onMatch = onMatch.map(_.rewrite(f))
  )

  def symbolTableDependencies =
    (expectations.flatMap(_.symbolTableDependencies)
      ++ onCreate.flatMap(_.symbolTableDependencies)
      ++ onMatch.flatMap(_.symbolTableDependencies)).toSet - identifier

  def children: Seq[AstNode[_]] = expectations ++ onCreate ++ onMatch
}