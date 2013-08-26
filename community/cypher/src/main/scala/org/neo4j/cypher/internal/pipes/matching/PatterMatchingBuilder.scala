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
package org.neo4j.cypher.internal.pipes.matching

import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commands.Predicate
import collection.Map
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.pipes.QueryState
import org.neo4j.cypher.internal.data.{Entity, RelationshipThingie, NodeThingie}

class PatterMatchingBuilder(patternGraph: PatternGraph, predicates: Seq[Predicate]) extends MatcherBuilder {
  def getMatches(sourceRow: ExecutionContext, state: QueryState): Traversable[ExecutionContext] = {
    val bindings: Map[String, Any] = sourceRow.filter(_._2.isInstanceOf[Entity])
    val boundPairs: Map[String, MatchingPair] = extractBoundMatchingPairs(bindings)

    val undirectedBoundRelationships: Iterable[PatternRelationship] = bindings.keys.
      filter(z => patternGraph.contains(z)).
      filter(patternGraph(_).isInstanceOf[PatternRelationship]).
      map(patternGraph(_).asInstanceOf[PatternRelationship]).
      filter(_.dir == Direction.BOTH)

    val mandatoryPattern: Traversable[ExecutionContext] = if (undirectedBoundRelationships.isEmpty) {
      createPatternMatcher(boundPairs, false, sourceRow, state)
    } else {
      val boundRels: Seq[Map[String, MatchingPair]] =
        createListOfBoundRelationshipsWithHangingNodes(undirectedBoundRelationships, bindings, state)

      boundRels.map(relMap => createPatternMatcher(relMap ++ boundPairs, false, sourceRow, state)).reduceLeft(_ ++ _)
    }

    if (patternGraph.containsOptionalElements)
      mandatoryPattern.flatMap(innerMatch => createPatternMatcher(extractBoundMatchingPairs(innerMatch), true, sourceRow, state))
    else
      mandatoryPattern
  }

  private def createListOfBoundRelationshipsWithHangingNodes(undirectedBoundRelationships: Iterable[PatternRelationship], bindings: Map[String, Any], state:QueryState): Seq[Map[String, MatchingPair]] = {
    val toList = undirectedBoundRelationships.map(patternRel => {
      val rel = bindings(patternRel.key).asInstanceOf[RelationshipThingie]
      val x = patternRel.key -> MatchingPair(patternRel, rel)

      // Outputs the first direction of the pattern relationship
      val a1 = patternRel.startNode.key -> MatchingPair(patternRel.startNode, state.query.getStartNode(rel.id))
      val a2 = patternRel.endNode.key -> MatchingPair(patternRel.endNode, state.query.getEndNode(rel.id))

      // Outputs the second direction of the pattern relationship
      val b1 = patternRel.startNode.key -> MatchingPair(patternRel.startNode, state.query.getEndNode(rel.id))
      val b2 = patternRel.endNode.key -> MatchingPair(patternRel.endNode, state.query.getStartNode(rel.id))

      Seq(Map(x, a1, a2), Map(x, b1, b2))
    }).toList
    cartesian(toList).map(_.reduceLeft(_ ++ _))
  }

  private def createNullValuesForOptionalElements(matchedGraph: ExecutionContext): ExecutionContext = {
    val m = (patternGraph.keySet -- matchedGraph.keySet).map(_ -> null).toStream
    matchedGraph.newWith(m)
  }

  // This method takes  a Seq of Seq and produces the cartesian product of all inner Seqs
  // I'm committing this code, but it's all Tobias' doing.
  private def cartesian[T](lst: Seq[Seq[T]]): Seq[Seq[T]] =
    lst.foldRight(List(List[T]()))(// <- the type T needs to be specified here
      (element: Seq[T], result: List[List[T]]) => // types for better readability
        result.flatMap(r => element.map(e => e :: r))
    ).toSeq

  private def createPatternMatcher(boundPairs: Map[String, MatchingPair], includeOptionals: Boolean, source: ExecutionContext, state:QueryState): Traversable[ExecutionContext] = {
    val patternMatcher = if (patternGraph.hasDoubleOptionals)
      new DoubleOptionalPatternMatcher(boundPairs, predicates, includeOptionals, source, state, patternGraph.doubleOptionalPaths)
    else
      new PatternMatcher(boundPairs, predicates, includeOptionals, source, state)

    if (includeOptionals)
      patternMatcher.map(matchedGraph => matchedGraph ++ createNullValuesForOptionalElements(matchedGraph))
    else
      patternMatcher
  }

  private def extractBoundMatchingPairs(bindings: Map[String, Any]): Map[String, MatchingPair] = bindings.flatMap {
    case (key, value: Entity) if patternGraph.contains(key) =>
      val element = patternGraph(key)

      value match {
        case node: NodeThingie        => Seq(key -> MatchingPair(element, node))
        case rel: RelationshipThingie => {
          val pr = element.asInstanceOf[PatternRelationship]

          val x = pr.dir match {
            case Direction.OUTGOING => Some((pr.startNode, pr.endNode))
            case Direction.INCOMING => Some((pr.endNode, pr.startNode))
            case Direction.BOTH     => None
          }

          //We only want directed bound relationships. Undirected relationship patterns
          //have to be treated a little differently
          x match {
            case Some((a, b)) => {
              val t1 = a.key -> MatchingPair(a, null)
              val t2 = b.key -> MatchingPair(b, null)
              val t3 = pr.key -> MatchingPair(pr, rel)

              Seq(t1, t2, t3)
            }
            case None         => Nil
          }
        }
      }


    case _ => Nil
  }
}

