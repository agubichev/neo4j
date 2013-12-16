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
package org.neo4j.cypher.internal.compiler.v2_0.ast

import org.scalatest.FunSuite
import org.neo4j.cypher.internal.compiler.v2_0.InputToken

class AstRewriteTest extends FunSuite {

  abstract class TestExp extends AstNode {
    def token: InputToken = ???
  }

  case class Add(a: TestExp, b: TestExp) extends TestExp
  case class Num(x: Int) extends TestExp
  case class Maybe(x: Option[TestExp]) extends TestExp
  case class Sequence(x: Seq[TestExp]) extends TestExp
  case class MaybeSequence(x: Seq[Option[TestExp]]) extends TestExp
  case class Mapping(x: Map[String, TestExp]) extends TestExp

  val collapseAddition: PartialFunction[AstNode, AstNode] = {
    case Add(Num(a), Num(b)) => Num(a + b)
  }

  test("should collapse addition") {
    //given
    val add = Add(Num(1), Num(2))

    //when
    val result = add.rewrite(collapseAddition)

    //then
    assert(result == Num(3))
  }

  test("should collapse addition inside an option") {
    //given
    val ast = Maybe(Some(Add(Num(1), Num(2))))

    //when
    val result = ast.rewrite(collapseAddition)

    assert(result == Maybe(Some(Num(3))))
  }

  test("should collapse addition inside an seq") {
    //given
    val ast = Sequence(Seq(Add(Num(1), Num(2))))

    //when
    val result = ast.rewrite(collapseAddition)

    assert(result == Sequence(Seq(Num(3))))
  }

  test("should collapse addition inside an option inside a seq") {
    //given
    val ast = MaybeSequence(Seq(Some(Add(Num(1), Num(2)))))

    //when
    val result = ast.rewrite(collapseAddition)

    assert(result == MaybeSequence(Seq(Some(Num(3)))))
  }

  test("should collapse addition inside a map") {
    //given
    val ast = Mapping(Map("a" -> Add(Num(1), Num(2))))

    //when
    val result = ast.rewrite(collapseAddition)

    assert(result == Mapping(Map("a" -> Num(3))))
  }
}

