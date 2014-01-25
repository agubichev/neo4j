package org.neo4j.cypher.internal.compiler.v2_1.planner

/*
 * Copyright (C) 2012 Neo Technology
 * All rights reserved
 */

trait PlanGeneratorTest {
  case class Plan(coveredIds: Set[Id], name: String, effort: Cost) extends AbstractPlan {
    def lhs: Option[AbstractPlan] = ???

    def rhs: Option[AbstractPlan] = ???
  }

  def table(plans: Plan*): PlanTable = new PlanTable(plans)

  def plan(ids: Set[Int], name: String, effort: Int) = Plan(ids.map(Id.apply), name, Cost(effort, 1))

}

