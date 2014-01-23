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

import org.junit.{Ignore, After, Before, Test}
import org.neo4j.cypher.internal.compiler.v2_0.newCompiler._
import org.neo4j.graphdb._
import scala.collection.mutable
import org.neo4j.tooling.GlobalGraphOperations
import java.lang.Iterable
import org.neo4j.kernel.impl.util.PrimitiveLongIterator
import org.neo4j.cypher.internal.compiler.v2_0.newCompiler.ExpandOp
import org.neo4j.cypher.internal.compiler.v2_0.newCompiler.StatementContext
import org.neo4j.cypher.internal.compiler.v2_0.newCompiler.LabelScanOp
import org.neo4j.cypher.internal.compiler.v2_0.newCompiler.AllNodesScanOp
import org.neo4j.cypher.internal.compiler.v2_0.spi.QueryContext
import org.neo4j.cypher.internal.spi.v2_0.TransactionBoundExecutionContext

class OperatorTest extends ExecutionEngineHelper {

  var tx: Transaction = _
  var stx: StatementContext = _
  var ctx: QueryContext = _

  @Before def init() {
    tx = graph.beginTx()
    stx = StatementContext(statement)
    ctx = new TransactionBoundExecutionContext(graph, tx, statement)
  }

  @After def after() {
    tx.close()
  }

  @Ignore
  @Test def all_nodes_on_empty_database() {
    val allNodesScan = AllNodesScanOp(stx, 0, new mutable.HashMap[Int, Any]())
    allNodesScan.open()
    assert(allNodesScan.next() === false, "Expected not to find any nodes")
  }

  @Ignore
  @Test def all_nodes_on_database_with_three_nodes() {
    val data = new mutable.HashMap[Int, Any]()
    val allNodesScan = AllNodesScanOp(stx, 0, data)

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

    val allNodesScan = LabelScanOp(stx, registerIdx, labelToken, data)

    assert(allNodesScan.toList(data) === List(Map(0 -> a1.getId), Map(0 -> a2.getId)))
  }

  @Test def expand() {
    val data = new mutable.HashMap[Int, Any]()

    val source = createLabeledNode("A")
    val destination = createNode()
    val relId = relate(source, destination).getId

    val sourceId = 0
    val destinationId = 1

    val lhs = {
      val labelToken = 0
      LabelScanOp(stx, sourceId, labelToken, data)
    }
    val expand = ExpandOp(ctx, sourceId, destinationId, lhs, data)

    assert(expand.toList(data) === List(Map(0 -> source.getId, 1 -> destinationId)))
  }


  @Test def hash_join() {
    val data = new mutable.HashMap[Int, Any]()

    for (i <- 0.until(10)) {
      val middle = createLabeledNode("A", "B")
      val lhs = createLabeledNode("A")
      val rhs = createLabeledNode("B")

      if (i >= 2) {
        relate(lhs, middle)
      }

      if (i < 8) {
        relate(rhs, middle)
      }
    }

    val id1 = 0
    val id2 = 1
    val joinKeyId = 2
    val labelAToken = 0
    val labelBToken = 1

    val labelScan1 = LabelScanOp(stx, id1, labelAToken, data)
    val lhs = ExpandOp(ctx, id1, joinKeyId, labelScan1, data)

    val labelScan2 = LabelScanOp(stx, id2, labelBToken, data)
    val rhs = ExpandOp(ctx, id2, joinKeyId, labelScan2, data)

    val hashJoin = HashJoinOp(stx, joinKeyId, Seq(id1), Seq(id2), lhs, rhs, data)

    assert(hashJoin.toList(data).size === 6)
  }

  @Ignore @Test def performance_of_expand() {
    val data = new mutable.HashMap[Int, Any]()

    for (i <- 0.until(10000)) {
      val source = createLabeledNode("A")
      for (j <- 0.until(10)) {
        val destination = createNode()
        relate(source, destination)
      }
    }
    tx.success()
    tx.close()
    tx = graph.beginTx()

    val sourceId = 0
    val destinationId = 1
    val labelToken = 0

    var opsTime = new Counter(20)
    var cypherTime= new Counter(20)
    var coreTime = new Counter(20)
    var inlinedTime = new Counter(20)

    (0 until 100) foreach {
      x =>
        val start = System.nanoTime()

        val lhs = {
          LabelScanOp(stx, sourceId, labelToken, data)
        }
        val expand = ExpandOp(ctx, sourceId, destinationId, lhs, data)

        var count = 0
        expand.open()
        while (expand.next()) {
          count += 1
        }
        expand.close()
        val end = System.nanoTime()

        val duration = (end - start) / 1000000
        println(s"new operator time: ${duration} count: ${count}")
        opsTime += duration
    }

    (0 until 100) foreach {
      x =>
        val start = System.nanoTime()
        val count = execute("match (a:A)-[r]->() return r").size
        val end = System.nanoTime()

        val duration = (end - start) / 1000000
        println(s"old cypher time: ${duration} count: ${count}")
        cypherTime += duration
    }

    val label = DynamicLabel.label("A")

    (0 until 100) foreach {
      x =>
        val start = System.nanoTime()
        val allNodes: ResourceIterable[Node] = GlobalGraphOperations.at(graph).getAllNodesWithLabel(label)
        val nodes: ResourceIterator[Node] = allNodes.iterator()
        var count = 0
        while (nodes.hasNext) {
          val current = nodes.next()
          val relationships = current.getRelationships(Direction.OUTGOING).iterator()
          while(relationships.hasNext()) {
            count += 1
            relationships.next()
          }
        }
        val end = System.nanoTime()

        val duration = (end - start) / 1000000
        println(s"core time: ${duration} count: ${count}")
        coreTime += duration
    }

    (0 until 100) foreach {
      x =>
        val start = System.nanoTime()
        var count = 0
        val labeledNodes: PrimitiveLongIterator = stx.read.nodesGetForLabel(labelToken)
        while(labeledNodes.hasNext) {
          val current = labeledNodes.next()
          val rels = stx.read.relationshipsGetFromNode(current, Direction.OUTGOING)
          while(rels.hasNext) {
            count += 1
            rels.next()
          }
        }

        val end = System.nanoTime()

        val duration = (end - start) / 1000000
        println(s"inlined time: ${duration} count: ${count}")
        inlinedTime += duration
    }

    println(s"ops: ${opsTime.avg}")
    println(s"cypher: ${cypherTime.avg}")
    println(s"core: ${coreTime.avg}")
    println(s"inline: ${inlinedTime.avg}")
  }

  class Counter(skip: Int) {
    var sum: Double = 0.0
    var count = -skip

    def +=(v: Double) {
      if (count >=0) {
        sum += v
      }
      count += 1
    }

    def avg = sum/count
  }
}