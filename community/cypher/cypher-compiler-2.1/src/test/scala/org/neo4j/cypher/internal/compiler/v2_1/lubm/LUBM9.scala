package org.neo4j.cypher.internal.compiler.v2_1.lubm

import org.neo4j.cypher.internal.compiler.v2_1.pipes.QueryStateHelper
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.{ast, LabelId}
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_1.ast.{Identifier, RelTypeName}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Expand

/**
 * Andrey Gubichev, 18/07/14.
 */
class LUBM9 extends LUBMHelper{


  def handWrittenPlan:LogicalPlan = {
    val queryState = QueryStateHelper.queryStateFrom(graph, db.beginTx()).query
    val labelCourse = queryState.getLabelId("Course")
    val nodeScan1: LogicalPlan = NodeByLabelScan(IdName("z"), Right(LabelId(labelCourse)))
    val expand1: LogicalPlan = Expand(nodeScan1, IdName("z"),
      Direction.INCOMING, Seq(RelTypeName("TeacherOf") _), IdName("y"), IdName("p3"), SimplePatternLength)
    val selection1: LogicalPlan = Selection(Seq(ast.HasLabels(Identifier("y")_,Seq(ast.LabelName("Professor")_))_), expand1)

    val expand2: LogicalPlan = Expand(selection1, IdName("y"),
      Direction.INCOMING, Seq(RelTypeName("Advisor") _), IdName("x"), IdName("p1"), SimplePatternLength)
    val selection2: LogicalPlan = Selection(Seq(ast.HasLabels(Identifier("x")_,Seq(ast.LabelName("Student")_))_), expand2)
    val expand3: LogicalPlan = Expand(selection2, IdName("x"),
      Direction.OUTGOING, Seq(RelTypeName("TakesCourse") _), IdName("z1"), IdName("p2"), SimplePatternLength)
    val selection3: LogicalPlan = Selection(Seq(ast.Equals(Identifier("z")_,Identifier("z1")_)_),expand3)


    val projection = Projection(selection3,
      Map("z"->Identifier("z")_,"y"->Identifier("y")_, "x"->Identifier("x")_))
    return projection
  }

  def handWrittenHashJoinPlan:LogicalPlan = { //TODO: bug somewhere
    val queryState = QueryStateHelper.queryStateFrom(graph, db.beginTx()).query
    val labelStud = queryState.getLabelId("Student")
    val labelProf = queryState.getLabelId("Professor")

    val nodeScan1: LogicalPlan = NodeByLabelScan(IdName("x"), Right(LabelId(labelStud)))
    val expand1: LogicalPlan = Expand(nodeScan1, IdName("y"),
      Direction.OUTGOING, Seq(RelTypeName("Advisor") _), IdName("y"), IdName("p1"), SimplePatternLength)
    val selection1: LogicalPlan = Selection(Seq(ast.HasLabels(Identifier("y")_,Seq(ast.LabelName("Professor")_))_), expand1)
    val expand2: LogicalPlan = Expand(selection1, IdName("x"),
      Direction.OUTGOING, Seq(RelTypeName("TakesCourse") _), IdName("z"), IdName("p2"), SimplePatternLength)
    //val selection2: LogicalPlan = Selection(Seq(ast.HasLabels(Identifier("z")_,Seq(ast.LabelName("Course")_))_), expand2)


    val nodeScan2: LogicalPlan = NodeByLabelScan(IdName("y1"), Right(LabelId(labelProf)))
    val expand3: LogicalPlan = Expand(nodeScan2, IdName("y1"),
      Direction.OUTGOING, Seq(RelTypeName("TeacherOf") _), IdName("z"), IdName("p3"), SimplePatternLength)
    val selection2: LogicalPlan = Selection(Seq(ast.HasLabels(Identifier("z")_,Seq(ast.LabelName("Course")_))_), expand3)

    val hashJoin: LogicalPlan =  NodeHashJoin(IdName("z"), expand2, selection2)
    val selection3: LogicalPlan = Selection(Seq(ast.Equals(Identifier("y")_,Identifier("y1")_)_), hashJoin)

    val projection = Projection(selection3,
      Map("z"->Identifier("z")_,"y"->Identifier("y")_, "x"->Identifier("x")_))

    projection
  }

  test("LUBM Query 9") {
    val query = "cypher 2.1.experimental match (x:Student)-[:Advisor]->(y:Professor)-[:TeacherOf]->(z:Course)," +
      "(x:Student)-[:TakesCourse]->(z:Course) return count(*)"
    compileAndRun(query)

   // var times = List.fill(50)(runExperiment(handWrittenPlan,"Expands"))
   // println(times)
   // println("median: "+median(times))


  }

}
