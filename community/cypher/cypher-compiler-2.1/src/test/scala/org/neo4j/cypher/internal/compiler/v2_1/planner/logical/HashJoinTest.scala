package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.LogicalPlanningTestSupport
import org.neo4j.graphdb.{Direction, GraphDatabaseService}
import org.neo4j.cypher.ExecutionEngine
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.{Monitors, LabelId}
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.PipeInfo
import org.neo4j.cypher.internal.compiler.v2_1.pipes.{QueryStateHelper, QueryState}
import org.neo4j.cypher.internal.compiler.v2_1.planner.execution.TruffleExecutionPlanBuilder
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.cypher.internal.compiler.v2_1.ast.{RelTypeName, Identifier}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Projection
import org.neo4j.cypher.internal.compiler.v2_1.LabelId
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.PipeInfo

/**
 * Andrey Gubichev, 2014.
 */
class HashJoinTest extends CypherFunSuite with LogicalPlanningTestSupport {
  // var db: GraphDatabaseService = null
  // var engine: ExecutionEngine = null
  // val kernelMonitors = new org.neo4j.kernel.monitoring.Monitors
  // val monitors = new Monitors(kernelMonitors)
  var db = new GraphDatabaseFactory().newEmbeddedDatabase("/Users/andrey/Desktop/benchmark-data");
  var engine = new ExecutionEngine(db)
  var graph = db.asInstanceOf[GraphDatabaseAPI]

  def median(s: Seq[Double])  =
  {
    val (lower, upper) = s.sortWith(_<_).splitAt(s.size / 2)
    if (s.size % 2 == 0) (lower.last + upper.head) / 2.0 else upper.head
  }

  def buildHashJoin: LogicalPlan = {
    val queryState = QueryStateHelper.queryStateFrom(graph, db.beginTx()).query
    val labelCompany = queryState.getLabelId("Company")
    val labelConcert = queryState.getLabelId("Concert")

    val nodeScan1: LogicalPlan = NodeByLabelScan(IdName("corp"), Right(LabelId(labelCompany)))
    val expand1: LogicalPlan = Expand(nodeScan1, IdName("corp"),
      Direction.INCOMING, Seq(RelTypeName("SIGNED_WITH") _), IdName("a1"), IdName("p1"), SimplePatternLength)

    val nodeScan2: LogicalPlan = NodeByLabelScan(IdName("c"), Right(LabelId(labelConcert)))
    val expand2: LogicalPlan = Expand(nodeScan2, IdName("c"),
      Direction.INCOMING, Seq(RelTypeName("PERFORMED_AT") _), IdName("a1"), IdName("p2"), SimplePatternLength)

    val join: LogicalPlan = NodeHashJoin(IdName("a1"), expand1, expand2)
    val p = Projection(join, Map (
      "a1" -> Identifier("a1")_ ,
      "c"  -> Identifier("c")_ ,
      "corp" -> Identifier("corp")_
    ))
    p
  }

  def runExperiment(plan: LogicalPlan, explain: String):Double = {
    val truffleExecutionPlanBuilder = new TruffleExecutionPlanBuilder(monitors)
    val execPlan: PipeInfo = truffleExecutionPlanBuilder.build(plan)
    val t0 = System.nanoTime: Double
    val res = execPlan.pipe.createResults(QueryStateHelper.queryStateFrom(graph, db.beginTx())).toList
    val t1 = System.nanoTime: Double
    //println(explain + ": Elapsed time " + (t1 - t0) / 1000000.0 + " msecs")
    //println(explain + ": Result size " + res.size)
    (t1-t0)/1000000.0
  }

  test("should build plans containing hash joins") {
    val hashPlan = buildHashJoin
    var times = List.fill(500)(runExperiment(hashPlan,"Hash-Join handwritten"))
    //for (a <- 1 until 50) {
    //  runExperiment(hashPlan, "Hash-Join handwritten plan")
    //}
    println(times)
    println("median: "+median(times))

    val query = "cypher 2.1.experimental MATCH (corp:Company)<-[p1:SIGNED_WITH]-(a1:Artist)-[p2:PERFORMED_AT]->(c:Concert) return  corp, a1, c"
    //"-[p2:PERFORMED_AT]->(c:Concert) RETURN a1, corp"
    var t0: Double = 0
    var t1: Double = 0
    times = List.empty
    for (a <- 1 until 500) {
      t0 = System.nanoTime: Double
      val res = engine.execute(query).toList
      t1 = System.nanoTime: Double
      times = (t1-t0)/ 1000000.0 :: times
      //println("Compiled plan: Elapsed time " + (t1 - t0) / 1000000.0 + " msecs")
      //println("Compiled plan: Result size " + res.size)
    }

    println("median: " + median(times))

  }
}