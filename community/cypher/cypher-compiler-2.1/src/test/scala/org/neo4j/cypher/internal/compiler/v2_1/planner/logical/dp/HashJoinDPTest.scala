package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.dp

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.LogicalPlanningTestSupport
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_1.ast.NotEquals
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Selection
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Projection
import org.neo4j.cypher.internal.compiler.v2_1.ast.NotEquals
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Expand

/**
 * Created by andrey on 25/05/14.
 */
class HashJoinDPTest extends CypherFunSuite with LogicalPlanningTestSupport{
  test("should build plans containing joins") {
    implicit val planContext = newMockedPlanContext
    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case AllNodesScan(_) => 100
      case Expand(AllNodesScan(IdName("c")), IdName("c"), _, _, IdName("b"), _,_) => 3
      case Expand(AllNodesScan(IdName("a")), IdName("a"), _, _, IdName("b"), _,_) => 100
      case _: Expand                            => 100000
      case _: NodeHashJoin                      => 20
      case _                                    => Double.MaxValue
    })
    implicit val planner = newDPPlanner(factory)

    produceLogicalPlan("MATCH (a)<-[r1]-(b)-[r2]->(c) RETURN b") should equal(
      Projection(
        Selection(
          Seq(NotEquals(Identifier("r1")_,Identifier("r2")_)_),
          NodeHashJoin("b",
            Expand(AllNodesScan("c"), "c", Direction.INCOMING, Seq(), "b", "r2", SimplePatternLength),
            Expand(AllNodesScan("a"), "a", Direction.INCOMING, Seq(), "b", "r1", SimplePatternLength)
          )
        ),
        expressions = Map("b" -> Identifier("b")_)
      )
    )
  }

}
