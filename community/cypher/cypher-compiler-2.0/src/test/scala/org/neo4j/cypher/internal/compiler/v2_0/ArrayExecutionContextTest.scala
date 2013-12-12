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
package org.neo4j.cypher.internal.compiler.v2_0

import org.scalatest._

class ArrayExecutionContextTest extends FunSuite with BeforeAndAfter {

  var ctx: ArrayExecutionContext = _

  before {
    ctx = new ArrayExecutionContext()
  }

  test("A new ArrayExecutionContext should be empty") {
    assert(ctx.slots == Set.empty)
  }

  test("should fail when asked for non-existing value") {
    intercept[NoSuchElementException] {
      ctx("missing")
    }
  }

  test("should be able to store and retrieve a value") {
    ctx.update("X", 42)
    assert(ctx("X") == 42)
  }

  test("should be able to overwrite a value") {
    val FIRST_VALUE = 42
    val SECOND_VALUE = 666
    ctx.update("X", FIRST_VALUE)
    ctx.update("X", SECOND_VALUE)
    assert(ctx("X") == SECOND_VALUE)
  }

  test("added fields should show up in slots") {
    ctx.update("X", 1)
    ctx.update("Y", 2)
    assert(ctx.slots == Set("X", "Y"))
  }

  test("contains should answer no for missing fields") {
    assert(!ctx.contains("X"), "Should not contain field")
    ctx.update("X", 2)
    assert(ctx.contains("X"), "Should contain field")
  }

  test("get should return the expected values") {
    assert(ctx.get("X") == None)
    ctx.update("X", 2)
    assert(ctx.get("X") == Some(2))
  }

  test("getOrElse should run the else method when value is missing") {
    var elsed = false
    ctx.getOrElse("X", elsed = true)

    assert(elsed, "Expected else to have run already")
  }

  test("getOrElse should not run the else method when value is present") {
    ctx.update("X", 42)
    var elsed = false
    ctx.getOrElse("X", elsed = true)

    assert(!elsed, "Expected else to not have run already")
  }

  test("collect should see all keys and values") {
    ctx.update("X", 42)
    ctx.update("Y", "Apa")
    ctx.update("XXX", 666)

    val result = ctx.collect {
      case (key, value) if key.startsWith("X") => value
    }

    assert(result == List(42, 666))
  }

  test("collectValues should see values") {
    ctx.update("X", 42)
    ctx.update("Y", "Apa")
    ctx.update("XXX", 666)
    ctx.update("YYY", "Foo")

    val result = ctx.collectValues {
      case value: String => value
    }

    assert(result == List("Apa", "Foo"))
  }

  test("toMap should see values") {
    ctx.update("X", 42)
    ctx.update("Y", "Apa")
    ctx.update("XXX", 666)
    ctx.update("YYY", "Foo")

    val result = Map(
      "X" -> 42,
      "Y" -> "Apa",
      "XXX" -> 666,
      "YYY" -> "Foo")
    assert(ctx.toMap == result)
  }

  test("toMap on empty context should be empty") {
    assert(ctx.toMap == Map.empty)
  }

  test("copy should not affect the original") {
    ctx.update("X", 42)

    val newCtx = ctx.copy()

    newCtx.update("X", 666)

    assert(ctx("X") == 42)
    assert(newCtx("X") == 666)
  }

  test("when created with keys, should still know when a value is missing") {
    val ctx = new ArrayExecutionContext(Seq("X"))
    assert(ctx.get("X").isEmpty, "Expected this to be empty")
  }

  test("once a value is set, it should show up") {
    val ctx = new ArrayExecutionContext(Seq("X"))
    ctx.update("X", 1)
    assert(ctx.get("X").nonEmpty, "Expected this to not be empty")
  }

  test("toMap returns empty map until values are set") {
    val ctx = new ArrayExecutionContext(Seq("X"))
    assert(ctx.toMap == Map.empty)
  }
}
