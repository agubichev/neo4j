package org.neo4j.cypher.internal.compiler.v2_1.lubm

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{IdName, NodeIndexSeek, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v2_1.pipes.QueryStateHelper

/**
 * Andrey Gubichev, 17/07/14.
 */
class LUBM7 extends LUBMHelper{

  def handWrittenPlan: LogicalPlan = {
    val queryState = QueryStateHelper.queryStateFrom(graph, db.beginTx()).query
    val labelDept = queryState.getLabelId("Department")
    val labelStudent = queryState.getLabelId("GraduateStudent")
    val labelUni = queryState.getLabelId("University")
    ???
    //val nodeIndex = NodeIndexSeek(IdName("x"))
  }

  test("LUBM Query 7") {
    val query = "cypher 2.1.experimental MATCH (x)-[:TakesCourse]->(y)<-[:TeacherOf]-(z:Professor)" +
      "where z.id = '<http://www.Department0.University0.edu/AssociateProfessor0>' return y,z"
    compileAndRun(query)
    // TODO: add types
  }

}
