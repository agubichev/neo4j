package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.QueryPlan

class DPQueryGraphSolver(config: PlanningStrategyConfiguration = PlanningStrategyConfiguration.default)
  extends QueryGraphSolver {

  def plan(implicit context: QueryGraphSolvingContext, leafPlan: Option[QueryPlan] = None): QueryPlan = {
    println("DP ordering")
    val select = config.applySelections.asFunctionInContext
    val pickBest = config.pickBestCandidate.asFunctionInContext
    val dpTable = DPTable(Map.empty, pickBest)

    def generateLeafPlanTable() = {
      val leafPlanCandidateLists = config.leafPlanners.candidateLists(context.queryGraph)
      val leafPlanCandidateListsWithSelections = leafPlanCandidateLists.map(_.map(select))
      val bestLeafPlans: Iterable[QueryPlan] = leafPlanCandidateListsWithSelections.flatMap(pickBest(_))
      val startTable: DPTable = leafPlan.foldLeft(DPTable.getEmpty(pickBest))(_ + _)
      bestLeafPlans.foldLeft(startTable)(_ + _)
    }

    def expandTable(plans: Seq[DPTable], planGenerator: CandidateGenerator[Seq[QueryPlan]]): CandidateList = {
      plans.zipWithIndex.foldLeft(CandidateList()) { case (acc: CandidateList, (leftPlans: DPTable, size)) =>
        acc ++ plans.drop(size).foldLeft(CandidateList()) { case (acc: CandidateList, rightPlans: DPTable) =>
          acc ++ planGenerator(leftPlans.plans ++ rightPlans.plans)
        }
      }
    }

    val leaves = generateLeafPlanTable()
    val generated: Seq[DPTable] = (1 to context.queryGraph.coveredIds.size).foldLeft(Seq(leaves)) { case (dpTables: Seq[DPTable], i: Int) =>
      val candidates = expandTable(dpTables, expandsOrJoins)
      val newTable: DPTable = candidates.plans.foldLeft(DPTable(pickBestCandidate = pickBest))((a: DPTable, b: QueryPlan) => a + b)
      dpTables :+ newTable
    }
    generated.last.plans.find(p => p.solved.graph.coversSameNodesAs(context.queryGraph)).get
  }
}