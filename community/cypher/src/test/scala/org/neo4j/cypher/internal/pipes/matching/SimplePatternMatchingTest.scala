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

import org.junit.Test
import org.neo4j.cypher.internal.commands.{True, RelatedTo}
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.pipes.{NullDecorator, QueryState}
import org.neo4j.cypher.ExecutionEngineHelper
import org.neo4j.cypher.internal.executionplan.builders.PatternGraphBuilder
import org.neo4j.cypher.internal.symbols.{SymbolTable, RelationshipType, NodeType}
import org.neo4j.cypher.internal.spi.gdsimpl.GDSBackedQueryContext

class SimplePatternMatchingTest extends ExecutionEngineHelper with PatternGraphBuilder {
  val symbols = new SymbolTable(Map("a" -> NodeType()))

  def queryState = new QueryState(graph, new GDSBackedQueryContext(graph), Map.empty, NullDecorator)

  @Test def should_only_return_matches_that_fulfill_the_uniqueness_constraint() {
    // Given MATCH (a)--(b)--(c)
    val r1 = RelatedTo("a", "b", "r1", Seq.empty, Direction.BOTH, false, True())
    val r2 = RelatedTo("b", "c", "r2", Seq.empty, Direction.BOTH, false, True())

    val patternGraph = buildPatternGraph(symbols, Seq(r1, r2))
    val matcher = new SimplePatternMatcherBuilder(patternGraph, Seq(), symbols)
    val n0 = createNode()
    val n1 = createNode()
    relate(n0, n1)

    // When
    val result = matcher.getMatches(ExecutionContext.empty.newWith("a" -> n0), queryState).toList

    // Then
    assert(result === List.empty)
  }

  @Test def should_exclude_matches_overlapping_previously_found_relationships() {
    // Given MATCH (a)-[r1]-(b)-[r2]-(c)
    val r2 = RelatedTo("b", "c", "r2", Seq.empty, Direction.BOTH, false, True())

    val symbolTable = symbols.add("b", NodeType()).add("r", RelationshipType())
    val patternGraph: PatternGraph = buildPatternGraph(symbolTable, Seq(r2))
    val matcher = new SimplePatternMatcherBuilder(patternGraph, Seq(), symbolTable)
    val n0 = createNode()
    val n1 = createNode()
    val rel = relate(n0, n1)

    val startingState = ExecutionContext.empty.newWith(Map("a" -> n0, "b" -> n1, "r" -> rel))

    // When
    val result = matcher.getMatches(startingState, queryState).toList

    // Then
    assert(result === List.empty)
  }
}