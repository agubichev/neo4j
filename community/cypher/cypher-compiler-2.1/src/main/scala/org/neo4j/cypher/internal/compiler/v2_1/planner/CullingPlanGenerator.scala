package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.neo4j.cypher.internal.compiler.v2_1.spi.PlanContext

/*
To eagerly drop plans, we first order the plans by cost. Going from cheapest to most expensive, for every plan found,
call it X, go over the other plans and remove any plan that is covered by X. Here is an example. We start with the
table already ordered by cost.
 */
case class CullingPlanGenerator() extends PlanGenerator {
  def generatePlan(planContext: PlanContext, qg: QueryGraph, planTable: PlanTable): PlanTable = {

    if (planTable.size < 2)
      return planTable

    val sortedPlans: Seq[AbstractPlan] = sortPlans(planTable)
    val culledPlans = cullPlans(null, sortedPlans)

    PlanTable(culledPlans)
  }


  def sortPlans(planTable: PlanTable): Seq[AbstractPlan] = {
    planTable.m.sortWith {
      case (plan1, plan2) => plan1.effort < plan2.effort
    }
  }

  private def cullPlans(current: AbstractPlan, plans: Seq[AbstractPlan]): Seq[AbstractPlan] =
    if (plans.size < 2 || current == plans.last)
      plans
    else {
      val nextPlan = plans.apply(plans.indexOf(current) + 1)
      val newPlans = plans removePlansCoveredBy nextPlan
      cullPlans(nextPlan, newPlans)
    }

  implicit class RichPlan(inner: Seq[AbstractPlan]) {
    def removePlansCoveredBy(that: AbstractPlan) = inner.filter {
      case plan if that.covers(plan) && that != plan => false
      case _ => true
    }
  }
}
