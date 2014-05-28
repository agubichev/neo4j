package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{QueryPlan, IdName}
import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.QueryPlanProducer._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import scala.Some
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.QueryPlan
import org.neo4j.cypher.internal.compiler.v2_1.planner.PlannerQuery

/**
 * Created by andrey on 27/05/14.
 */
case class DPTable(m: Map[Set[IdName], QueryPlan] = Map.empty, pickBestCandidate: CandidateList => Option[QueryPlan]) {

  def +(newPlan: QueryPlan): DPTable = {
    val existingPlan = m.values.find(p => newPlan.solved.graph.coversSameNodesAs(p.solved.graph))

    existingPlan match {
      case Some(existingPlan) => {
        val bestCandidate = pickBestCandidate(CandidateList(Seq(existingPlan, newPlan)))
        if (bestCandidate.get.equals(newPlan)) {
          val oldPlansNotCoveredByNewPlan = m.filter {
            case (_, existingPlan) => !newPlan.solved.graph.coversSameNodesAs(existingPlan.solved.graph)
          }
          DPTable(oldPlansNotCoveredByNewPlan + (newPlan.availableSymbols -> newPlan), pickBestCandidate)
        } else {
          this
        }
      }
      case None => DPTable(m + (newPlan.availableSymbols -> newPlan), pickBestCandidate)
    }
  }

  def size = m.values.size

  def plans: Seq[QueryPlan] = m.values.toSeq


  def getFinalPlan(query: PlannerQuery): QueryPlan = {
    m.values.find(p => p.solved.graph.coversSameNodesAs(query.graph)).getOrElse(planSingleRow())
  }
}

object DPTable {
  def getEmpty(pickBestCandidate: CandidateList => Option[QueryPlan]) = {
    new DPTable(Map.empty, pickBestCandidate)
  }
}
