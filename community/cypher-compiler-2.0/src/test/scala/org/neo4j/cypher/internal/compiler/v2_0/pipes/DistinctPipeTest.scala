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
package org.neo4j.cypher.internal.compiler.v2_0.pipes

import org.junit.Test
import org.scalatest.Assertions
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.{Literal, Multiply, Expression, Identifier}
import org.neo4j.cypher.internal.compiler.v2_0.symbols.NumberType

class DistinctPipeTest extends Assertions {

  @Test def distinct_input_passes_through() {
    //GIVEN
    val pipe = createDistinctPipe(List(Map("x" -> 1), Map("x" -> 2)))

    //WHEN
    val result = pipe.createResults(QueryStateHelper.empty)

    //THEN
    assert(result.toList === List(Map("x" -> 1), Map("x" -> 2)))
  }

  @Test def distinct_executes_expressions() {
    //GIVEN
    val expressions = Map("doubled" -> Multiply(Identifier("x"), Literal(2)))
    val pipe = createDistinctPipe(List(Map("x" -> 1), Map("x" -> 2)), expressions)

    //WHEN
    val result = pipe.createResults(QueryStateHelper.empty)

    //THEN
    assert(result.toList === List(Map("doubled" -> 2), Map("doubled" -> 4)))
  }

  @Test def undistinct_input_passes_through() {
    //GIVEN
    val pipe = createDistinctPipe(List(Map("x" -> 1), Map("x" -> 1)))

    //WHEN
    val result = pipe.createResults(QueryStateHelper.empty)

    //THEN
    assert(result.toList === List(Map("x" -> 1)))
  }

  def createDistinctPipe(input: List[Map[String, Int]], expressions: Map[String, Expression] = Map("x" -> Identifier("x"))) = {
    val source = new FakePipe(input, "x" -> NumberType())
    new DistinctPipe(source, expressions)
  }
}

/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/SemanticErrorTest.scala
/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/SyntaxExceptionTest.scala
/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/TypeTest.scala
/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/commands/HasRelationshipTest.scala
/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/pipes/QueryStateHelper.scala
/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/commands/LabelActionTest.scala
/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/commands/PathExpressionTest.scala
/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/executionplan/ExecutionPlanBuilderTest.scala
/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/executionplan/builders/TrailBuilderTest.scala
/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/executionplan/builders/TrailDecomposeTest.scala
/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/executionplan/builders/TrailToStepTest.scala
/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/executionplan/builders/TraversalMatcherBuilderTest.scala
/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/mutation/CreateNodeActionTest.scala
/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/mutation/CreateRelationshipTest.scala
/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/mutation/DoubleCheckCreateUniqueTest.scala
/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/mutation/MapPropertySetActionTest.scala
/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/pipes/AllShortestPathsPipeTest.scala
/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/pipes/MutationTest.scala
/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/pipes/NamedPathPipeTest.scala
/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/pipes/PipeLazynessTest.scala
/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/pipes/SingleShortestPathPipeTest.scala
/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/pipes/matching/MatchingContextTest.scala
/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/pipes/matching/PatternMatchingTest.scala
/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/pipes/matching/PatternNodeTest.scala
/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/pipes/matching/ScalaPatternMatchingTest.scala
/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/pipes/matching/SimplePatternMatchingTest.scala
/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/pipes/matching/TraversalMatcherTest.scala
/Users/ata/dev/neo/neo4j/community/cypher-compiler-2.0/src/test/scala/org/neo4j/cypher/internal/compiler/v2_0/pipes/matching/VariableLengthExpanderStepExpandTest.scala
