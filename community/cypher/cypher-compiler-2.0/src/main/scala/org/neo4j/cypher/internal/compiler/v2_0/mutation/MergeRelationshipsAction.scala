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

import org.neo4j.cypher.internal.compiler.v2_0.commands.{Predicate, AstNode}
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_0.symbols._
import org.neo4j.cypher.internal.compiler.v2_0.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_0.symbols.SymbolTable
import org.neo4j.cypher.internal.compiler.v2_0.pipes.QueryState
/*

TODO:

* Given a seq of merge links, build follow-up steps and choose how to continue
* Convert AbstractPattern to Seq[MergeLink]

*/

final case class MergeLink(start: NodeMergeEntity, rel: RelationshipMergeEntity, relType: String, end: NodeMergeEntity)
  extends AstNode[MergeLink] with TypeSafe {

  def apply(ctx: ExecutionContext)(implicit qs: QueryState): MergeStep = {
    (ctx.contains(start.identifier), ctx.contains(rel.identifier), ctx.contains(end.identifier)) match {
      case (true, true, true) => MergeStep.DoNothing
      case (true, false, false) => MergeStep.Traverse( () => Iterator.empty)
      case (false, false, true) => MergeStep.Traverse( () => Iterator.empty)
      // Foo!
    }
  }

  def identifiers: Seq[(String, CypherType)] = start.identifiers ++ rel.identifiers ++ end.identifiers

  def children: Seq[AstNode[_]] = start.children ++ rel.children ++ end.children

  def rewrite(f: (Expression) => Expression): MergeLink =
    MergeLink(start.rewrite(f), rel.rewrite(f), relType, end.rewrite(f))

  def throwIfSymbolsMissing(symbols: SymbolTable): Unit = {
    start.throwIfSymbolsMissing(symbols)
    rel.throwIfSymbolsMissing(symbols)
    end.throwIfSymbolsMissing(symbols)
  }

  def symbolTableDependencies: Set[String] =
    start.symbolTableDependencies ++ rel.symbolTableDependencies ++ end.symbolTableDependencies
}

object MergeLink {

  def select(links: Set[MergeLink], lowestPriority: Int)
            (ctx: ExecutionContext)(implicit qs: QueryState): (Set[MergeLink], Option[(MergeLink, MergeStep)]) = {

      var nextLink: Option[(MergeLink, MergeStep)] = None
      var bestPrio = Int.MaxValue
      var newLinks = Set.newBuilder[MergeLink]

      for ( link <- links ) {
        val linkStep = link.apply(ctx)
        val linkPrio = linkStep.priority

        if ( linkPrio >= lowestPriority ) {
            if ( linkPrio < bestPrio ) {
              nextLink match {
                case Some((previousBestLink, _)) => newLinks += previousBestLink
                case _                           =>
              }
              nextLink = Some(link -> linkStep)
              bestPrio = linkPrio
            } else {
              newLinks += link
            }
        }
      }

    (newLinks.result(), nextLink)
  }
}

sealed abstract class MergeEntity extends AstNode[MergeEntity] with TypeSafe {
  def identifier: String

  def expectations: Seq[Predicate]
  def onCreate: Seq[UpdateAction]
  def onMatch: Seq[UpdateAction]

  def identifiers: Seq[(String, CypherType)]

  def children: Seq[AstNode[_]] = expectations ++ onCreate ++ onMatch

  def throwIfSymbolsMissing(symbols: SymbolTable): Unit = {
    expectations.foreach(_.throwIfSymbolsMissing(symbols))
    onCreate.foreach(_.throwIfSymbolsMissing(symbols))
    onMatch.foreach(_.throwIfSymbolsMissing(symbols))
  }

  def symbolTableDependencies: Set[String] =
    ( expectations.map(_.symbolTableDependencies) ++
      onCreate.map(_.symbolTableDependencies) ++
      onMatch.map(_.symbolTableDependencies) ).reduce(_++_)
}

final case class NodeMergeEntity(identifier: String,
                                 expectations: Seq[Predicate],
                                 onCreate: Seq[UpdateAction],
                                 onMatch: Seq[UpdateAction]) extends MergeEntity
{
  def identifiers: Seq[(String, CypherType)] = Seq((identifier, NodeType()))

  def rewrite(f: (Expression) => Expression): NodeMergeEntity =
    NodeMergeEntity(identifier, expectations.map(_.rewrite(f)), onCreate.map(_.rewrite(f)), onMatch.map(_.rewrite(f)))
}

final case class RelationshipMergeEntity(identifier: String,
                                         expectations: Seq[Predicate],
                                         onCreate: Seq[UpdateAction],
                                         onMatch: Seq[UpdateAction])  extends MergeEntity
{
  def identifiers: Seq[(String, CypherType)] = Seq((identifier, RelationshipType()))

  def rewrite(f: (Expression) => Expression): RelationshipMergeEntity =
    RelationshipMergeEntity(
      identifier, expectations.map(_.rewrite(f)), onCreate.map(_.rewrite(f)), onMatch.map(_.rewrite(f)))
}


sealed trait MergeStep {
  def priority: Int
}

sealed trait ApplicableMergeStep extends MergeStep {
  def execute(): Iterator[ExecutionContext]
}

object MergeStep {
  case object DoNothing extends MergeStep {
    def priority = 0
  }

  final case class Traverse( override val execute: () => Iterator[ExecutionContext]) extends ApplicableMergeStep {
    def priority = Traverse.priority
  }

  object Traverse { def priority = 1 }

  final case class Create( override val execute: () => Iterator[ExecutionContext]) extends ApplicableMergeStep {
    def priority = Create.priority
  }

  object Create { def priority = 2 }

  case object NotApplicable extends MergeStep {
    def priority = 3
  }
}


case class MergeRelationshipsAction(links: Seq[MergeLink]) extends UpdateAction {

  val initialLinkSet = links.toSet

  def children: Seq[AstNode[_]] = links.flatMap(_.children)

  def exec(context: ExecutionContext, state: QueryState): Iterator[ExecutionContext] = ???

  def throwIfSymbolsMissing(symbols: SymbolTable): Unit = links.foreach(_.throwIfSymbolsMissing(symbols))

  def identifiers: Seq[(String, CypherType)] = links.flatMap(_.identifiers)

  def rewrite(f: (Expression) => Expression): UpdateAction = MergeRelationshipsAction(links.map(_.rewrite(f)))

  def symbolTableDependencies: Set[String] = links.map(_.symbolTableDependencies).reduce(_++_)
}


//case class MergeRelationshipsAction(startNodeIdentifier: String,
//                                    endNodeIdentifier: String,
//                                    identifier: String,
//                                    relType: String,
//                                    expectations: Seq[Predicate],
//                                    onCreate: Seq[UpdateAction],
//                                    onMatch: Seq[UpdateAction]) extends UpdateAction {
//  def exec(context: ExecutionContext, state: QueryState): Iterator[ExecutionContext] = {
//    val startNode = CastSupport.castOrFail[Node](context(startNodeIdentifier))
//    val endNode = CastSupport.castOrFail[Node](context(endNodeIdentifier))
//    val foundRelationships: Iterator[ExecutionContext] = findRelationships(context, startNode, endNode)(state)
//
//    if (foundRelationships.isEmpty) {
//      val rel = state.query.createRelationship(start = startNode, end = endNode, relType = relType)
//
//      val newContext = context += (identifier -> rel)
//      onCreate.foreach(action => action.exec(newContext, state))
//
//      Iterator(newContext)
//    } else {
//      foundRelationships.map {
//        nextContext =>
//          onMatch.foreach(_.exec(nextContext, state))
//          nextContext
//      }
//    }
//  }
//
//  def findRelationships(context: ExecutionContext, startNode: Node, endNode: Node)(implicit state: QueryState): Iterator[ExecutionContext] = {
//    val startIter: Iterator[Relationship] = state.query.getRelationshipsFor(startNode, Direction.OUTGOING, Seq(relType))
//
//    def isConnectedToEndnode = (r: Relationship) => r.getEndNode == endNode
//    def insertIntoExecContext = (r: Relationship) => context.newWith(identifier -> r)
//    def isOkWithExpectations = (ctx: ExecutionContext) => expectations.forall(predicate => predicate.isMatch(ctx))
//
//    startIter.
//    filter(isConnectedToEndnode).
//    map(insertIntoExecContext).
//    filter(isOkWithExpectations)
//  }
//
//  def throwIfSymbolsMissing(in: SymbolTable): Unit = {
//
//    if (in.keys.contains(identifier))
//      throw new CypherTypeException(identifier + " already defined.")
//
//    val symbols = in.add(identifier, RelationshipType())
//
//    expectations.foreach(_.throwIfSymbolsMissing(symbols))
//    onCreate.foreach(_.throwIfSymbolsMissing(symbols))
//    onMatch.foreach(_.throwIfSymbolsMissing(symbols))
//  }
//
//  def identifiers: Seq[(String, CypherType)] = Seq(identifier -> RelationshipType())
//
//  def rewrite(f: (Expression) => Expression): UpdateAction = MergeRelationshipsAction(
//    startNodeIdentifier = startNodeIdentifier,
//    endNodeIdentifier = endNodeIdentifier,
//    identifier = identifier,
//    relType = relType,
//    expectations = expectations.map(_.rewrite(f)),
//    onCreate = onCreate.map(_.rewrite(f)),
//    onMatch = onMatch.map(_.rewrite(f))
//  )
//
//  def symbolTableDependencies =
//    (expectations.flatMap(_.symbolTableDependencies)
//      ++ onCreate.flatMap(_.symbolTableDependencies)
//      ++ onMatch.flatMap(_.symbolTableDependencies)).toSet - identifier
//
//  def children: Seq[AstNode[_]] = expectations ++ onCreate ++ onMatch
//}