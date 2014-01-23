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
package org.neo4j.cypher.internal.compiler.v2_0.newCompiler

import org.neo4j.cypher.internal.compiler.v2_0.spi.PlanContext
import org.neo4j.graphdb.Direction

case class PartialPlan(lhs: Set[Id], rhs: Set[Id], plan: PhysicalPlan) {
  def update(current: Option[PartialPlan]): Option[PartialPlan] = current match {
    case None                                         => Some(this)
    case Some(best) if best.plan.effort > plan.effort => Some(this)
    case _                                            => current
  }   
}

class PlanGenerator(estimator: CostEstimator) {
  def findImprovedPartialPlan(planContext: PlanContext, table: PlanTable, qg: QueryGraph): PartialPlan = {
    var result: Option[PartialPlan] = None
    
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

      // find extensions
      for (id <- ids) {
        for (rel <- qg.graphRelsById(id)) {
          val otherId = rel.other(id)
          
          if (! ids(otherId)) {
            // construct plan and candidate
            val relTypeTokens = rel.types.map(_(planContext))
            val expandedPlan = ExpandRelationships(plan, rel.direction, estimator.costForExpandRelationship(Seq.empty, relTypeTokens, rel.direction))
            val candidate = PartialPlan(ids, Set(otherId), expandedPlan)
            
            result = candidate.update(result)
          }
        }
      }

    }
    
    result.get
  }

  def generatePlan(planContext: PlanContext, qg: QueryGraph): PhysicalPlan = {
    var currentPlan: PlanTable = buildInitialTable(planContext, qg)

    while(currentPlan.m.size > 1) {
      val cheapestNewPlan: PartialPlan = findImprovedPartialPlan(planContext, currentPlan, qg)
      currentPlan = (currentPlan += cheapestNewPlan)
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
            LabelScan(id, tokenId, estimator.costForScan(tokenId))
        }

        val plan = if ( labelScans.isEmpty )
        {
          AllNodesScan(id, estimator.costForAllNodes())
        }
        else
        {
          labelScans.head
        }

        Set(id) -> plan
    }.toMap
    PlanTable(m)
  }
}

case class Cost(cardinality: Int, effort: Int) extends Ordered[Cost] {
  // TODO: Include effort here
  def compare(that: Cost): Int = cardinality - that.cardinality
}

trait CostEstimator {
  def costForScan(labelId: Token): Cost
  def costForAllNodes(): Cost
  def costForExpandRelationship(labelId: Seq[Token], relationshipType: Seq[Token], dir: Direction): Cost
}

case class PlanTable(m: Map[Set[Id], PhysicalPlan]) {
  def plan: PhysicalPlan = m.values.head

  def apply(nodes: Set[Id]): PhysicalPlan = m(nodes)

  def +=(plan: PartialPlan): PlanTable = {
    var builder = Map.newBuilder[Set[Id], PhysicalPlan]
    for ( pair @ (k, v) <- m if k != plan.lhs && k != plan.rhs) {
      builder += pair
    }
    builder += (plan.lhs ++ plan.rhs) -> plan.plan
    PlanTable(builder.result())
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

  def effort: Cost
}

trait PhysicalPlanLeaf {
  self: PhysicalPlan =>

  def lhs: Option[PhysicalPlan] = None
  def rhs: Option[PhysicalPlan] = None
}

case class AllNodesScan(id: Id, effort: Cost) extends PhysicalPlan with PhysicalPlanLeaf

case class LabelScan(id: Id, label: Token, effort: Cost) extends PhysicalPlan with PhysicalPlanLeaf

case class ExpandRelationships(left: PhysicalPlan, direction: Direction, effort: Cost) extends PhysicalPlan {
  def lhs: Option[PhysicalPlan] = Some(left)
  def rhs: Option[PhysicalPlan] = None
}
