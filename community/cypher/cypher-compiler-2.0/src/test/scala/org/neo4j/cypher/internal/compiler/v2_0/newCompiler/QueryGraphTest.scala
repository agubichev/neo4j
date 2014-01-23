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

import org.scalatest.Assertions
import org.junit.Test
import org.parboiled.scala._
import org.neo4j.cypher.internal.compiler.v2_0.parser.Query
import org.neo4j.cypher.internal.compiler.v2_0.ast
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_0.ast._

class QueryGraphTest extends Assertions with Query {

  private def parse(input: String): ast.Query =
    ReportingParseRunner(Query ~ EOI).run(input).result match {
      case Some(ast) => ast
      case None => fail("oh noes!")
    }

  @Test def simple_pattern_with_two_nodes() {
    val astObject = parse("match (a)-->(b) return *")
    val edges = Seq(GraphRelationship(Id(0), Id(1), Direction.OUTGOING, Seq.empty))
    val projection = Seq.empty

    val result = QueryGraphBuilder.build(astObject)

    assert(result === QueryGraph(Id(1), edges, Seq.empty, projection))
  }

  @Test def simple_pattern_with_three_nodes() {
    val astObject = parse("match (a)-->(b)-->(c) return *")
    val edges = Seq(GraphRelationship(Id(0), Id(1), Direction.OUTGOING, Seq.empty), GraphRelationship(Id(1), Id(2), Direction.OUTGOING, Seq.empty))
    val projection = Seq.empty

    val result = QueryGraphBuilder.build(astObject)

    assert(result === QueryGraph(Id(2), edges, Seq.empty, projection))
  }

  @Test def labeled_nodes() {
    val astObject = parse("match (a:Foo)-->(b:Bar) return *")
    val edges = Seq(GraphRelationship(Id(0), Id(1), Direction.OUTGOING, Seq.empty))
    val projection = Seq.empty

    val result = QueryGraphBuilder.build(astObject)

    assert(result === QueryGraph(Id(1), edges, Seq(
      Id(0) -> NodeLabelSelection(Label("Foo")),
      Id(1) -> NodeLabelSelection(Label("Bar"))), projection))
  }
}

object QueryGraphBuilder {
  def build(query: ast.Query): QueryGraph = query match {
    case ast.SingleQuery(clauses, _) =>
      clauses.head match {
        case ast.Match(_, Pattern(patterns: Seq[PatternPart], _), _, _, _) =>

          var knownNodes = Map[String, Id]()
          var lastNode = -1

          def extractElement(pattern: PatternPart) = pattern match {
            case EveryPath(element) => element
          }

          def addNode(node: NodePattern): (Id, Seq[(Id, Selection)]) = {
            val (id: Id, labels: Seq[Identifier]) = node match {
              case AnonymousNodePattern(labels, _, _, _) =>
                lastNode = lastNode + 1
                (Id(lastNode), labels)

              case NamedNodePattern(Identifier(nodeName, _), labels, _, _, _) => knownNodes.getOrElse(nodeName, {
                lastNode = lastNode + 1
                val nodeId = Id(lastNode)
                knownNodes = knownNodes + (nodeName -> nodeId)
                (nodeId, labels)
              })
            }
            val selections = labels.map(label =>
              (id, NodeLabelSelection(Label(label.name)))
            )
            (id, selections)
          }

          def extractNodesAndEdges(pattern: PatternElement): (Id, Seq[(Id, Selection)], Seq[GraphRelationship]) = pattern match {
            case node: NodePattern =>
              val (id, selections) = addNode(node)
              (id, selections, Seq.empty)

            case RelationshipChain(element, rel, rNode: NodePattern, _) =>
              val (lhs, lhsSelections, relationships) = extractNodesAndEdges(element)
              val (rhs, rhsSelections) = addNode(rNode)
              (rhs, lhsSelections ++ rhsSelections, relationships ++ Seq(GraphRelationship(lhs, rhs, rel.direction, Seq.empty)))
          }

          val (edges: Seq[Seq[GraphRelationship]], selections: Seq[Seq[(Id, Selection)]]) = patterns.map(p => {
            val (_, selections, relationships) = extractNodesAndEdges(extractElement(p))
            (relationships, selections)
          }).unzip

          QueryGraph(Id(lastNode), edges.flatten, selections.flatten, Seq.empty)
      }
    case _ =>
      ???
  }
}