package org.neo4j.cypher.internal.compiler.v2_0.newCompiler

import org.neo4j.cypher.internal.compiler.v2_0.spi.PlanContext
import org.neo4j.graphdb.Direction

class PlanGenerator(estimator: CostEstimator) {
  def joinPlans(table: PlanTable, qg: QueryGraph): JoinPlanResult = {
//    for(leftIds <- table.m.keys;
//        rightIds <- table.m.keys) {
//
//      qg.optJoinMethods(leftIds, rightIds) match {
//        case Some(methods) =>
//          val leftPlan = table(leftIds)
//          val rightPlan = table(rightIds)
//        case None =>
//      }
//    }
    for (ids <- table.m.keys) {
      val plan = table(ids)

    }
  }

  def generatePlan(planContext: PlanContext, qg: QueryGraph): PhysicalPlan = {
    var currentPlan: PlanTable = buildInitialTable(planContext, qg)

    while(currentPlan.m.size > 1) {
      val cheapestNewPlan: JoinPlanResult = joinPlans(currentPlan)
      currentPlan = currentPlan.add(cheapestNewPlan)
    }

    currentPlan.plan
  }

  private def buildInitialTable(planContext: PlanContext, qg: QueryGraph) = {
    val m = qg.maxId.allIncluding.map {
      id =>
        val selections: Seq[Selection] = qg.selectionsByNode(id)
        val labelScans = selections.collect {
          case NodeLabelSelection(label) =>
            val tokenId = Token(planContext.getLabelId(label.name))
            val cardinality = estimator.cardinalityForScan(tokenId)
            LabelScan(id, tokenId, cardinality)
        }

        val plan = if (labelScans.isEmpty)
          AllNodesScan(id, estimator.cardinalityForAllNodes())
        else
          labelScans.head

        Set(id) -> plan
    }.toMap
    PlanTable(m)
  }
}

case class JoinPlanResult(lhs: Set[Id], rhs: Set[Id], plan: PhysicalPlan)

trait CostEstimator {
  def cardinalityForScan(labelId: Token): Int

  def cardinalityForAllNodes(): Int

  def cardinalityForRelationshipExpand(labelId: Token, relationshipType: Token, dir: Direction): Int
}

case class PlanTable(m: Map[Set[Id], PhysicalPlan]) {
  def apply(nodes: Set[Id]): PhysicalPlan = m(nodes)

  def plan: PhysicalPlan = m.values.head

  def add(plan: JoinPlanResult): PlanTable =
    PlanTable(((m - plan.lhs) - plan.rhs) + ((plan.lhs ++ plan.rhs) -> plan.plan))
}

/**
 * The physical plan. Stateless thing that knows how to create the operator tree which
 * is actually execute.
 */
trait PhysicalPlan {
  def lhs: Option[PhysicalPlan]

  def rhs: Option[PhysicalPlan]

  def createPhysicalPlan(): Operator = ???

  def cost: Int
}

trait Operator

trait PhysicalPlanLeaf {
  self: PhysicalPlan =>

  def lhs: Option[PhysicalPlan] = None

  def rhs: Option[PhysicalPlan] = None
}

case class AllNodesScan(id: Id, cost: Int) extends PhysicalPlan with PhysicalPlanLeaf

case class LabelScan(id: Id, label: Token, cost: Int) extends PhysicalPlan with PhysicalPlanLeaf

case class Expand(left: PhysicalPlan, direction: Direction, cost: Int) extends PhysicalPlan {
  def lhs: Option[PhysicalPlan] = Some(left)

  def rhs: Option[PhysicalPlan] = None
}