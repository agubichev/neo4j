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
package org.neo4j.cypher

import org.junit.Test
import org.neo4j.graphdb.Node

class UniquenessAcceptanceTest extends ExecutionEngineHelper {
  @Test def my_friend_of_a_friend_query_should_not_return_me() {
    relate(createNode("Me"), createNode("Bob"))
    val result = parseAndExecute("MATCH a-->()-->b WHERE a.name = 'Me' RETURN b.name")
    assert(List() === result.toList)
  }

  // n1-->n2, n2-->n2
  // a--b-->c--d
  @Test def x {

  }

  //
}
