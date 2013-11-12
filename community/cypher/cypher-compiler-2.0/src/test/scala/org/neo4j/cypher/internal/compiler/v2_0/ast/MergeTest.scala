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

import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.cypher.internal.compiler.v2_0.{SemanticState, DummyToken}
import org.neo4j.cypher.internal.compiler.v2_0.symbols.{CypherType, NodeType}
import org.neo4j.cypher.internal.compiler.v2_0.parser
import org.parboiled.scala.parserunners.BasicParseRunner
import org.parboiled.scala.rules.Rule1

class MergeTest extends Assertions {

  object MergeParser extends parser.Clauses {
    def Clause: Rule1[Clause] = ???

    def parse(text: String): Merge = BasicParseRunner(Merge).run(text).result.get
  }


  def semanticState(name: String, typ: CypherType) =
    SemanticState.clean.declareIdentifier(Identifier(name, DummyToken(1, 2)), typ).right.get


  @Test def should_not_accept_already_bound_identifiers() {
    val merge = MergeParser.parse("MERGE (a)")

    val state = semanticState("a", NodeType())
    val result = merge.semanticCheck(state)

    assert(result.errors.map(_.msg) === Seq("a already declared"))
  }
}
