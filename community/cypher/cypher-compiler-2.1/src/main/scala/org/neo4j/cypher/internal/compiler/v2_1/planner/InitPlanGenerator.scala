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
package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.neo4j.cypher.internal.compiler.v2_1.spi.PlanContext

case class InitPlanGenerator(estimator: CardinalityEstimator, calculator: CostCalculator) extends PlanGenerator {
  def generatePlan(planContext: PlanContext, qg: QueryGraph, planTable: PlanTable): PlanTable = {
    val m = qg.maxId.allIncluding.map {
      id =>
        val selections: Seq[Selection] = qg.selectionsByNode(id)
        val labelScans = selections.collect {
          case NodeLabelSelection(label) =>
            val tokenId: Token = planContext.labelGetId(label.name)
            val cardinality = estimator.estimateLabelScan(tokenId)
            LabelScan(id, tokenId, calculator.costForLabelScan(cardinality))
        }

        val plan = if (labelScans.isEmpty) {
          val cardinality = estimator.estimateAllNodes()
          AllNodesScan(id, calculator.costForAllNodes(cardinality))
        }
        else {
          labelScans.head
        }

        plan
    }
    PlanTable(m)

  }
}