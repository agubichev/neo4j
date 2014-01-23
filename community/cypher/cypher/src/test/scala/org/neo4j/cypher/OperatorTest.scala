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
package org.neo4j.cypher

import org.junit.{After, Before, Test}
import org.neo4j.cypher.internal.compiler.v2_0.newCompiler.{ExpandOp, LabelScanOp, AllNodesScanOp}
import org.neo4j.cypher.internal.compiler.v2_0.QueryStateHelper
import org.neo4j.graphdb.Transaction
import scala.collection.mutable
import org.neo4j.cypher.internal.compiler.v2_0.spi.QueryContext

class OperatorTest extends ExecutionEngineHelper {

  var tx: Transaction = _
  var qtx: QueryContext = _

  @Before def init() {
    tx = graph.beginTx()
    qtx = QueryStateHelper.queryStateFrom(graph, tx).query
  }

  @After def after() {
    tx.close()
  }

  @Test def all_nodes_on_empty_database() {
    val allNodesScan = AllNodesScanOp(qtx, 0, new mutable.HashMap[Int, Any]())
    allNodesScan.open()
    assert(allNodesScan.next() === false, "Expected not to find any nodes")
  }

  @Test def all_nodes_on_database_with_three_nodes() {
    val data = new mutable.HashMap[Int, Any]()
    val allNodesScan = AllNodesScanOp(qtx, 0, data)

    createNode()
    createNode()
    createNode()

    assert(allNodesScan.toList(data) === List(Map(0 -> 0), Map(0 -> 1), Map(0 -> 2)))
  }
  
  @Test def labelScan() {
    val data = new mutable.HashMap[Int, Any]()
    val registerIdx = 0
    val labelToken = 0

    createNode()
    val a1 = createLabeledNode("A")

    createLabeledNode("B")
    val a2 = createLabeledNode("A")

    val allNodesScan = LabelScanOp(qtx, registerIdx, labelToken, data)

    assert(allNodesScan.toList(data) === List(Map(0 -> a1.getId), Map(0 -> a2.getId)))
  }

  @Test def expand() {
    val data = new mutable.HashMap[Int, Any]()

    val source = createLabeledNode("A")
    val destination = createNode()
    relate(source, destination)

    val sourceId = 0
    val destinationId = 1

    val lhs = {
      val labelToken = 0
      LabelScanOp(qtx, sourceId, labelToken, data)
    }
    val expand = ExpandOp(qtx, sourceId, destinationId, lhs, data)

    assert(expand.toList(data) === List(Map(0 -> source.getId, 1 -> destination.getId)))
  }
}