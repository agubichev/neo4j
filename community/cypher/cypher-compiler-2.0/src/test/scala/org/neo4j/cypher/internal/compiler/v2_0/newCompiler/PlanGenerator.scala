package org.neo4j.cypher.internal.compiler.v2_0.newCompiler

import org.neo4j.cypher.internal.compiler.v2_0.spi.PlanContext
import org.neo4j.graphdb.Direction

class PlanGenerator(estimator: CardinalityEstimator, calculator: CostCalculator) {

  def tryCombineAllPlans(table: PlanTable, graph: QueryGraph)


  def joinPlans(table: PlanTable, qg: QueryGraph, planContext: PlanContext): JoinPlanResult = {
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

    //    val combinedPlans = tryCombineAllPlans(table, qg)
    val cheapestPlan: JoinPlanResult = expandPlans(table, qg, planContext).sortBy(_.plan.cost).head

    table.add(cheapestPlan)
  }

  private def expandPlans(table: PlanTable, qg: QueryGraph, planContext: PlanContext): Seq[JoinPlanResult] = {
    table.flatMap {
      case (ids: Set[Id], plan: PhysicalPlan) =>

        // Find query relationships where one end is outside the physical plan and one end is inside
        val expansions: Seq[GraphRelationship] = qg.edges.collect {
          case rel: GraphRelationship if ids.contains(rel.end) ^ ids.contains(rel.start) => rel
        }
        expansions.flatMap {
          case rel =>
            // Find known labels on this node, and calculate expected cardinality based on this
            createExpandOperators(qg, rel, planContext, plan) // TODO: This is NOT CORRECT! Find out how to combine cardinalities and test it correctly
        }
    }
  }

  private def createExpandOperators(qg: QueryGraph, rel: GraphRelationship, planContext: PlanContext, plan: PhysicalPlan): Any = {
    val expandCardinality = {
      val labelCardinalities: Seq[Int] = qg.selectionsByNode(rel.start).collect {
        case NodeLabelSelection(label) =>
          planContext.getOptLabelId(label).map {
            id =>
              val labelToken = LabelToken(id)
              estimator.cardinalityForRelationshipExpand(labelToken, rel.direction)
          }
      }

      if (labelCardinalities.isEmpty) {
        // We don't know about any labels used on this node.
        val cardinality = estimator.cardinalityForRelationshipExpand(rel.direction)
      } else
        labelCardinalities.reduce((a, b) => (a + b) / 2)
    }

    val cost = calculator.expand(expandCardinality)

    Expand(rel.start, plan, rel.direction, cost)
  }

  def generatePlan(planContext: PlanContext, qg: QueryGraph): PhysicalPlan = {
    var planTable: PlanTable = buildInitialTable(planContext, qg)

    while (planTable.m.size > 1) {
      val cheapestNewPlan: JoinPlanResult = joinPlans(planTable, qg, planContext)
      planTable = planTable.add(cheapestNewPlan)
    }

    planTable.plan
  }

  private def buildInitialTable(planContext: PlanContext, qg: QueryGraph) = {
    val m = qg.maxId.allIncluding.map {
      id =>
        val selections: Seq[Selection] = qg.selectionsByNode(id)
        val labelScans = selections.collect {
          case NodeLabelSelection(label) =>
            val tokenId = LabelToken(planContext.getLabelId(label.name))
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

case class JoinPlanResult(id: Set[Id], plan: PhysicalPlan) {
  def doesNotCover(ids: Set[Id]) = (ids intersect id).nonEmpty
}

trait CardinalityEstimator {
  def cardinalityForScan(labelId: LabelToken): Int
  def cardinalityForAllNodes(): Int

  def cardinalityForRelationshipExpand(labelId: LabelToken, relationshipType: TypeToken, dir: Direction): Int

  def cardinalityForRelationshipExpand(labelId: LabelToken, dir: Direction): Int

  def cardinalityForRelationshipExpand(relationshipType: TypeToken, dir: Direction): Int

  def cardinalityForRelationshipExpand(dir: Direction): Int

  def cardinalityForRelationshipExpand(): Int
}

trait CostCalculator {
  def expand(cardinality: Int): Int
}

case class PlanTable(m: Map[Set[Id], PhysicalPlan]) {
  def apply(nodes: Set[Id]): PhysicalPlan = m(nodes)

  def plan: PhysicalPlan = m.values.head

  def add(plan: JoinPlanResult): PlanTable = {
    m.filterKeys {
      ids =>
    }
  }
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

case class LabelScan(id: Id, label: LabelToken, cost: Int) extends PhysicalPlan with PhysicalPlanLeaf

case class Expand(fromNode: Id, left: PhysicalPlan, direction: Direction, cost: Int) extends PhysicalPlan {
  def lhs: Option[PhysicalPlan] = Some(left)

  def rhs: Option[PhysicalPlan] = None
}