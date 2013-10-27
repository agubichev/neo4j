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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders.prepare

import org.junit.Test
import org.neo4j.cypher.internal.compiler.v2_0.commands.Query
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions._
import org.neo4j.cypher.internal.compiler.v2_0.commands.values.UnresolvedProperty
import org.neo4j.cypher.internal.compiler.v2_0.commands.ReturnItem
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Add
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.CountStar
import org.neo4j.cypher.internal.compiler.v2_0.commands.SingleNode
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Property
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders.BuilderTest
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.{PartiallySolvedQuery, PlanBuilder}

class AggregationPreparationRewriterTest extends BuilderTest {

  def builder: PlanBuilder = AggregationPreparationRewriter(Some(namer))

  @Test
  def should_just_pass_through_queries_without_aggregation() {
    // given MATCH n RETURN n
    val q = Query.
      matches(SingleNode("n")).
      returns(ReturnItem(Identifier("n"), "n"))

    // when
    assertRejects(q)
  }

  @Test
  def should_split_query_up_into_two() {
    // given MATCH n RETURN n.foo + count(*)
    val q = Query.
      matches(n).
      returns(ReturnItem(Add(nFoo, countStar), addNFooCountStarString))

    // when
    val result = assertAccepts(q).query

    // then MATCH n WITH n.foo AS x1, count(*) AS x2 RETURN x1 + x2
    val expectedTail =
      Query.
        start().
        returns(ReturnItem(Add(Identifier(nFooString), Identifier(countStartString)), addNFooCountStarString))

    val expected = Query.
      matches(n).
      tail(expectedTail).
      returns(
      ReturnItem(nFoo, nFooString, renamed = true),
      ReturnItem(countStar, countStartString, renamed = true)
    )


    assert(result === PartiallySolvedQuery(expected))
  }

  @Test
  def should_follow_the_tails() {

    // given MATCH n WITH n RETURN n.foo + count(*)
    val nReturn = ReturnItem(Identifier("n"), "n")

    val originalTail = Query.
      start().
      returns(ReturnItem(Add(nFoo, countStar), addNFooCountStarString))

    val q = Query.
      matches(n).
      tail(originalTail).
      returns(nReturn)

    // when
    val result = assertAccepts(q).query

    // then
    // MATCH n WITH n //first
    // WITH n.foo AS x1, count(*) AS x2 //second
    // RETURN x1 + x2 //third
    val third =
      Query.
        start().
        returns(ReturnItem(Add(Identifier(nFooString), Identifier(countStartString)), addNFooCountStarString))

    val second = Query.
      start().
      tail(third).
      returns(
        ReturnItem(nFoo, nFooString, renamed = true),
        ReturnItem(countStar, countStartString, renamed = true))

    val first = Query.
      matches(n).
      tail(second).
      returns(nReturn)

    assert(result === PartiallySolvedQuery(first))
  }

  @Test
  def should_keep_other_expressions() {
    // given MATCH n RETURN n.bar, n.foo + count(*)
    val q = Query.
      matches(n).
      returns(
      ReturnItem(Add(nFoo, countStar), "add stuff"),
      ReturnItem(nBar, nBarString)
    )

    // when
    val result = assertAccepts(q).query

    // then MATCH n WITH n.bar as x1, n.foo AS x2, count(*) AS x3 RETURN x1, x2 + x3
    val expectedTail =
      Query.
        start().
        returns(
        ReturnItem(Add(Identifier(nFooString), Identifier(countStartString)), "add stuff"),
        ReturnItem(Identifier(nBarString), nBarString))

    val expected = Query.
      matches(n).
      tail(expectedTail).
      returns(
      ReturnItem(nBar, nBarString, renamed = true),
      ReturnItem(nFoo, nFooString, renamed = true),
      ReturnItem(countStar, countStartString, renamed = true)
    )

    val query = PartiallySolvedQuery(expected)
    assert(result === query)
  }

  val nFoo = Property(Identifier("n"), UnresolvedProperty("foo"))
  val nBar = Property(Identifier("n"), UnresolvedProperty("bar"))
  val n = SingleNode("n")
  val nFooString = "n.foo"
  val nBarString = "n.bar"
  val countStartString = "count(*)"
  val addNFooCountStarString = "n.foo + count(*)"
  val countStar = CountStar()
  val namer = Map[Expression, String](
    countStar -> countStartString,
    nFoo -> nFooString,
    nBar -> nBarString)
}