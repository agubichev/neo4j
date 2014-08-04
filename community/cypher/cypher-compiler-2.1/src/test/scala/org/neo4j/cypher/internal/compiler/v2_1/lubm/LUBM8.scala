package org.neo4j.cypher.internal.compiler.v2_1.lubm

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.pipes.QueryStateHelper
import org.neo4j.cypher.internal.compiler.v2_1.{PropertyKeyId, LabelId, ast}
import org.neo4j.cypher.internal.compiler.v2_1.ast.{RelTypeName, Identifier}
import org.neo4j.cypher.internal.compiler.v2_1.PropertyKeyId
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Projection
import org.neo4j.cypher.internal.compiler.v2_1.LabelId
import org.neo4j.graphdb.Direction

/**
 * Andrey Gubichev, 17/07/14.
 */
class LUBM8 extends LUBMHelper{

  def handWrittenPlan: LogicalPlan = {
    val queryState = QueryStateHelper.queryStateFrom(graph, db.beginTx()).query
    val labelUni = queryState.getLabelId("University")
    val propertyID = queryState.getPropertyKeyId("id");

    //val expr = ast.Equals(ast.Property(ast.Identifier("z")_,ast.PropertyKeyName("id")_)_,ast.Parameter("'<http://www.University0.edu>'")_)_
    val expr = ast.StringLiteral("<http://www.University0.edu>")_
    val nodeIndex = NodeIndexSeek(IdName("z"), LabelId(labelUni), PropertyKeyId(propertyID), expr)

    val expand1 =  Expand(nodeIndex, IdName("z"),
      Direction.INCOMING, Seq(RelTypeName("SubOrganizationOf")_), IdName("y"), IdName("p2"),VarPatternLength(1, Some(2)))

    val selection1: LogicalPlan = Selection(Seq(ast.HasLabels(Identifier("y")_,Seq(ast.LabelName("Department")_))_), expand1)
    val expand2 =  Expand(selection1, IdName("y"),
      Direction.INCOMING, Seq(RelTypeName("MemberOf")_), IdName("x"), IdName("p1"),SimplePatternLength)

    val projection = Projection(expand2,
    Map("z"->Identifier("z")_,"y"->Identifier("y")_, "x"->Identifier("x")_))
    return projection
  }


  test("LUBM Query 8") {
    val query = "cypher 2.1.experimental MATCH (y)-[p2:SubOrganizationOf*1..2]->(z:University)" +
      " where z.id = '<http://www.University0.edu>' return y"
    //compileAndRun(query)

    println("hand written plan: "+ handWrittenPlan)
    var times = List.fill(50)(runExperiment(handWrittenPlan,"Expands"))
    println(times)
    println("median: "+median(times))

  }

}
