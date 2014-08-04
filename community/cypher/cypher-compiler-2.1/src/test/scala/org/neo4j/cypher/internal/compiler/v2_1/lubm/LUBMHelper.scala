package org.neo4j.cypher.internal.compiler.v2_1.lubm

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.LogicalPlanningTestSupport
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.cypher.ExecutionEngine
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_1.planner.execution.{TrufflePushModelExecutionPlanBuilder, TruffleExecutionPlanBuilder}
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.PipeInfo
import org.neo4j.cypher.internal.compiler.v2_1.pipes.QueryStateHelper

/**
 * Andrey Gubichev, 17/07/14.
 */
class LUBMHelper extends CypherFunSuite with LogicalPlanningTestSupport{
  var db = new GraphDatabaseFactory().newEmbeddedDatabase(System.getProperty("LUBMpath")/*"/Users/andrey/Desktop/data/lubm50inf"*/);
  var engine = new ExecutionEngine(db)
  var graph = db.asInstanceOf[GraphDatabaseAPI]

  def median(s: Seq[Double])  =
  {
    val (lower, upper) = s.sortWith(_<_).splitAt(s.size / 2)
    if (s.size % 2 == 0) (lower.last + upper.head) / 2.0 else upper.head
  }
  def runExperiment(plan: LogicalPlan, explain: String):Double = {
    val truffleExecutionPlanBuilder = new TrufflePushModelExecutionPlanBuilder(monitors)
    val execPlan: PipeInfo = truffleExecutionPlanBuilder.build(plan)
    val t0 = System.nanoTime: Double
    val res = execPlan.pipe.createResults(QueryStateHelper.queryStateFrom(graph, db.beginTx())).toList
    val t1 = System.nanoTime: Double
    //println(explain + ": Elapsed time " + (t1 - t0) / 1000000.0 + " msecs")
    //println(explain + ": Result size " + res.size)
    (t1-t0)/1000000.0
  }


  def compileAndRun(query:String){
    var t0: Double = 0
    var t1: Double = 0
    val times: List[Double] = List.fill(100){
      t0 = System.nanoTime: Double
      val res = engine.execute(query).toList
      t1 = System.nanoTime: Double
      //times = (t1-t0)/ 1000000.0 :: times
      //println("Compiled plan: Elapsed time " + (t1 - t0) / 1000000.0 + " msecs")
      //println("Compiled plan: Result String Length " + s.size)
      //println("Compiled plan: Result size " + res.size)

      (t1-t0)/1000000.0
    }
    println(times)
    println("median: "+median(times))
  }
}
