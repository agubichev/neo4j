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

import org.neo4j.cypher.internal.compiler.v2_0._
import commands.SortItem
import commands.expressions.{Add, Literal, RandFunction, Identifier}
import symbols.{NumberType, StringType, AnyType}
import org.neo4j.cypher.PatternException
import org.junit.Test
import org.junit.Assert._
import org.scalatest.junit.JUnitSuite
import collection.mutable.{Map=>MutableMap}
import scala.util.Random

class SortPipeTest extends JUnitSuite {
  @Test def emptyInIsEmptyOut() {
    val source = new FakePipe(List(), "x" -> AnyType())
    val sortPipe = new SortPipe(source, List(SortItem(Identifier("x"), true)))

    assertEquals(List(), sortPipe.createResults(QueryStateHelper.empty).toList)
  }

  @Test def simpleSortingIsSupported() {
    val list:Seq[ExecutionContext] = List(ExecutionContext.from("x" -> "B"), ExecutionContext.from("x" -> "A"))
    val source = new FakePipe(list, "x" -> StringType())
    val sortPipe = new SortPipe(source, List(SortItem(Identifier("x"), true)))

    assertEquals(List(ExecutionContext.from("x" -> "A"), ExecutionContext.from("x" -> "B")), sortPipe.createResults(QueryStateHelper.empty).toList)
  }

  @Test def sortByTwoColumns() {
    val source = new FakePipe(List(
      ExecutionContext.from("x" -> "B", "y" -> 20),
      ExecutionContext.from("x" -> "A", "y" -> 100),
      ExecutionContext.from("x" -> "B", "y" -> 10)), "x" -> StringType(), "y"->NumberType())

    val sortPipe = new SortPipe(source, List(
      SortItem(Identifier("x"), true),
      SortItem(Identifier("y"), true)))

    assertEquals(List(
      ExecutionContext.from("x" -> "A", "y" -> 100),
      ExecutionContext.from("x" -> "B", "y" -> 10),
      ExecutionContext.from("x" -> "B", "y" -> 20)), sortPipe.createResults(QueryStateHelper.empty).toList)
  }

  @Test def sortByTwoColumnsWithOneDescending() {
    val source = new FakePipe(List(
      ExecutionContext.from("x" -> "B", "y" -> 20),
      ExecutionContext.from("x" -> "A", "y" -> 100),
      ExecutionContext.from("x" -> "B", "y" -> 10)), "x" -> StringType(), "y"->NumberType())

    val sortPipe = new SortPipe(source, List(
      SortItem(Identifier("x"), true),
      SortItem(Identifier("y"), false)))

    assertEquals(List(
      ExecutionContext.from("x" -> "A", "y" -> 100),
      ExecutionContext.from("x" -> "B", "y" -> 20),
      ExecutionContext.from("x" -> "B", "y" -> 10)), sortPipe.createResults(QueryStateHelper.empty).toList)
  }

  @Test def shouldHandleSortingWithNullValues() {
    val list: Seq[ExecutionContext] = List(
      ExecutionContext.from("y" -> 1),
      ExecutionContext.from("y" -> null),
      ExecutionContext.from("y" -> 2))
    val source = new FakePipe(list, "y"->NumberType())

    val sortPipe = new SortPipe(source, List(SortItem(Identifier("y"), true)))

    assertEquals(List(
      ExecutionContext.from("y" -> 1),
      ExecutionContext.from("y" -> 2),
      ExecutionContext.from("y" -> null)), sortPipe.createResults(QueryStateHelper.empty).toList)
  }

  @Test def shouldHandleSortingWithComputedValues() {
    val list:Seq[ExecutionContext] = List(
      ExecutionContext.from("x" -> 3),
      ExecutionContext.from("x" -> 1),
      ExecutionContext.from("x" -> 2))

    val source = new FakePipe(list, "x" -> NumberType())

    val sortPipe = new SortPipe(source, List(SortItem(Add(Identifier("x"), Literal(1)), true)))

    val actualResult = sortPipe.createResults(QueryStateHelper.empty).toList
    val expectedResult =  List(
      ExecutionContext.from("x" -> 1),
      ExecutionContext.from("x" -> 2),
      ExecutionContext.from("x" -> 3))
    assertEquals(expectedResult, actualResult)
  }

  @Test(expected = classOf[PatternException]) def shouldNotAllowSortingWithRandomValues() {
    val list:Seq[ExecutionContext] = Random.shuffle(
      for (v <- 1 to 1000)
      yield ExecutionContext.from("x" -> (v: Any)))

    val source = new FakePipe(list, "x" -> NumberType())

    val sortPipe = new SortPipe(source, List(SortItem(Add(Add(Literal(1), RandFunction()), Literal(1)), true)))

    sortPipe.createResults(QueryStateHelper.empty)
  }
}
