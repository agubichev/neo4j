package org.neo4j.cypher.internal.compiler.v2_1.lubm

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.LogicalPlanningTestSupport
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.cypher.ExecutionEngine
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.cypher.internal.compiler.v2_1.pipes.QueryStateHelper
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.{ast, LabelId}
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_1.ast.{Identifier, RelTypeName}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Expand
import org.neo4j.cypher.internal.compiler.v2_1.planner.execution.TruffleExecutionPlanBuilder
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.PipeInfo

/**
 * Andrey Gubichev, 17/07/14.
 */
class LUBM2 extends LUBMHelper{

  def handWrittenPlan = {
    val queryState = QueryStateHelper.queryStateFrom(graph, db.beginTx()).query
    val labelDept = queryState.getLabelId("Department")
    val labelStudent = queryState.getLabelId("GraduateStudent")
    val labelUni = queryState.getLabelId("University")

    val nodeScan1: LogicalPlan = NodeByLabelScan(IdName("z"), Right(LabelId(labelDept)))
    val expand1: LogicalPlan = Expand(nodeScan1, IdName("z"),
      Direction.INCOMING, Seq(RelTypeName("MemberOf") _), IdName("x"), IdName("p1"), SimplePatternLength)
    val selection1: LogicalPlan = Selection(Seq(ast.HasLabels(Identifier("x")_,Seq(ast.LabelName("GraduateStudent")_))_), expand1)

    val expand2: LogicalPlan = Expand(selection1, IdName("z"),
      Direction.OUTGOING, Seq(RelTypeName("SubOrganizationOf")_), IdName("y"), IdName("p3"),SimplePatternLength)
    val selection2: LogicalPlan = Selection(Seq(ast.HasLabels(Identifier("y")_,Seq(ast.LabelName("University")_))_), expand2)


    //val nodeScan2: LogicalPlan = NodeByLabelScan(IdName("y1"), Right(LabelId(labelUni)))
    //val expand3: LogicalPlan = Expand(nodeScan2, IdName("y1"),
    //  Direction.INCOMING, Seq(RelTypeName("UndergraduateDegreeFrom") _), IdName("x"), IdName("p2"), SimplePatternLength)
    //val selection3: LogicalPlan = Selection(Seq(ast.HasLabels(Identifier("x")_,Seq(ast.LabelName("GraduateStudent")_))_), expand3)

    val expand4: LogicalPlan = Expand(selection2, IdName("x"),
      Direction.OUTGOING, Seq(RelTypeName("UndergraduateDegreeFrom") _), IdName("y1"), IdName("p2"), SimplePatternLength)

    //val hashJoin: LogicalPlan = NodeHashJoin(IdName("x"), selection2, selection3)
    val selection4: LogicalPlan = Selection(Seq(ast.Equals(Identifier("y")_,Identifier("y1")_)_),expand4)

    val projection: LogicalPlan = Projection(selection4,
      Map("x"->Identifier("x")_, "z" -> Identifier("z")_, "y" -> Identifier("y")_))

    projection
  }

  def handWrittenPlan2 = {
    val queryState = QueryStateHelper.queryStateFrom(graph, db.beginTx()).query
    val labelDept = queryState.getLabelId("Department")
    val labelStudent = queryState.getLabelId("GraduateStudent")
    val labelUni = queryState.getLabelId("University")

    val nodeScan1: LogicalPlan = NodeByLabelScan(IdName("x"), Right(LabelId(labelStudent)))
    val expand1: LogicalPlan = Expand(nodeScan1, IdName("x"),
      Direction.OUTGOING, Seq(RelTypeName("MemberOf") _), IdName("z"), IdName("p1"), SimplePatternLength)
    val selection1: LogicalPlan = Selection(Seq(ast.HasLabels(Identifier("z")_,Seq(ast.LabelName("Department")_))_), expand1)
    val expand2: LogicalPlan = Expand(selection1, IdName("x"),
      Direction.OUTGOING, Seq(RelTypeName("UndergraduateDegreeFrom")_), IdName("y"), IdName("p2"),SimplePatternLength)
    val selection2: LogicalPlan = Selection(Seq(ast.HasLabels(Identifier("y")_,Seq(ast.LabelName("University")_))_), expand2)
    val expand3: LogicalPlan = Expand(selection2, IdName("z"),
      Direction.OUTGOING, Seq(RelTypeName("SubOrganizationOf") _), IdName("y1"), IdName("p3"), SimplePatternLength)
    val selection3: LogicalPlan = Selection(Seq(ast.Equals(Identifier("y1")_,Identifier("y")_)_),expand3)
    val projection: LogicalPlan = Projection(selection3,
      Map("x"->Identifier("x")_, "z" -> Identifier("z")_, "y" -> Identifier("y")_))
    projection
  }

  def handWrittenPlan3 = {
    val queryState = QueryStateHelper.queryStateFrom(graph, db.beginTx()).query
    val labelDept = queryState.getLabelId("Department")
    val labelStudent = queryState.getLabelId("GraduateStudent")
    val labelUni = queryState.getLabelId("University")

    val nodeScan1: LogicalPlan = NodeByLabelScan(IdName("y"), Right(LabelId(labelUni)))
    val expand1: LogicalPlan = Expand(nodeScan1, IdName("y"),
      Direction.INCOMING, Seq(RelTypeName("SubOrganizationOf") _), IdName("z"), IdName("p3"), SimplePatternLength)
    val selection1: LogicalPlan = Selection(Seq(ast.HasLabels(Identifier("z")_,Seq(ast.LabelName("Department")_))_), expand1)
    val expand2: LogicalPlan = Expand(selection1, IdName("y"),
      Direction.INCOMING, Seq(RelTypeName("UndergraduateDegreeFrom")_), IdName("x"), IdName("p2"),SimplePatternLength)
    val selection2: LogicalPlan = Selection(Seq(ast.HasLabels(Identifier("x")_,Seq(ast.LabelName("GraduateStudent")_))_), expand2)
    val expand3: LogicalPlan = Expand(selection2, IdName("x"),
      Direction.OUTGOING, Seq(RelTypeName("MemberOf") _), IdName("z1"), IdName("p1"), SimplePatternLength)
    val selection3: LogicalPlan = Selection(Seq(ast.Equals(Identifier("z")_,Identifier("z1")_)_),expand3)
    val projection: LogicalPlan = Projection(selection3,
      Map("x"->Identifier("x")_, "z" -> Identifier("z")_, "y" -> Identifier("y")_))
    projection
  }

  def handWrittenPlan4 = {
    val queryState = QueryStateHelper.queryStateFrom(graph, db.beginTx()).query
    val labelDept = queryState.getLabelId("Department")
    val labelStudent = queryState.getLabelId("GraduateStudent")
    val labelUni = queryState.getLabelId("University")

    val nodeScan1: LogicalPlan = NodeByLabelScan(IdName("z"), Right(LabelId(labelDept)))
    val expand1: LogicalPlan = Expand(nodeScan1, IdName("z"),
      Direction.OUTGOING, Seq(RelTypeName("SubOrganizationOf") _), IdName("y"), IdName("p3"), SimplePatternLength)
    val selection1: LogicalPlan = Selection(Seq(ast.HasLabels(Identifier("y")_,Seq(ast.LabelName("University")_))_), expand1)
    val expand2: LogicalPlan = Expand(selection1, IdName("z"),
      Direction.INCOMING, Seq(RelTypeName("MemberOf")_), IdName("x"), IdName("p2"),SimplePatternLength)
    val selection2: LogicalPlan = Selection(Seq(ast.HasLabels(Identifier("x")_,Seq(ast.LabelName("GraduateStudent")_))_), expand2)
    val expand3: LogicalPlan = Expand(selection2, IdName("x"),
      Direction.OUTGOING, Seq(RelTypeName("UndergraduateDegreeFrom") _), IdName("y1"), IdName("p1"), SimplePatternLength)
    val selection3: LogicalPlan = Selection(Seq(ast.Equals(Identifier("y")_,Identifier("y1")_)_),expand3)
    val projection: LogicalPlan = Projection(selection3,
      Map("x"->Identifier("x")_, "z" -> Identifier("z")_, "y" -> Identifier("y")_))
    projection
  }

  def handWrittenPlan5 = {
    val queryState = QueryStateHelper.queryStateFrom(graph, db.beginTx()).query
    val labelDept = queryState.getLabelId("Department")
    val labelStudent = queryState.getLabelId("GraduateStudent")
    val labelUni = queryState.getLabelId("University")

    val nodeScan1: LogicalPlan = NodeByLabelScan(IdName("x"), Right(LabelId(labelStudent)))
    val expand1: LogicalPlan = Expand(nodeScan1, IdName("x"),
      Direction.OUTGOING, Seq(RelTypeName("MemberOf") _), IdName("z"), IdName("p1"), SimplePatternLength)
    //val selection1: LogicalPlan = Selection(Seq(ast.HasLabels(Identifier("z")_,Seq(ast.LabelName("Department")_))_), expand1)
    val expand2: LogicalPlan = Expand(expand1, IdName("x"),
      Direction.OUTGOING, Seq(RelTypeName("UndergraduateDegreeFrom")_), IdName("y"), IdName("p2"),SimplePatternLength)
    //val selection2: LogicalPlan = Selection(Seq(ast.HasLabels(Identifier("y")_,Seq(ast.LabelName("University")_))_), expand2)

    val nodeScan2: LogicalPlan = NodeByLabelScan(IdName("y1"), Right(LabelId(labelUni)))
    val expand3: LogicalPlan = Expand(nodeScan2, IdName("y1"),
      Direction.INCOMING, Seq(RelTypeName("SubOrganizationOf") _), IdName("z"), IdName("p3"), SimplePatternLength)
    val selection1: LogicalPlan = Selection(Seq(ast.HasLabels(Identifier("z")_,Seq(ast.LabelName("Department")_))_), expand3)

    val hashJoin: LogicalPlan =  NodeHashJoin(IdName("z"), selection1, expand2)

    val selection3: LogicalPlan = Selection(Seq(ast.Equals(Identifier("y")_,Identifier("y1")_)_),hashJoin)

    val projection: LogicalPlan = Projection(selection3,
      Map("x"->Identifier("x")_, "z" -> Identifier("z")_, "y" -> Identifier("y")_))
    projection

  }

  test("LUBM Query 2") {
    val query = "cypher 2.1.experimental MATCH (x:GraduateStudent)-[p1:MemberOf]->(z:Department)-[p3:SubOrganizationOf]->(y:University)" +
      ", (x:GraduateStudent)-[p2:UndergraduateDegreeFrom]->(y:University) return x,y,z"
     // "(x:GraduateStudent)-[p2:UndergraduateDegreeFrom]->(y:University)<-[p3:SubOrganizationOf]-(z:Department) return x,y,z"
    compileAndRun(query)

    /*var times = List.fill(50)(runExperiment(handWrittenPlan2,"Students First"))
    println(times)
    println("median: "+median(times))

    times = List.fill(50)(runExperiment(handWrittenPlan5,"Students First + HashJoin"))
    println(times)
    println("median: "+median(times))

    times = List.fill(50)(runExperiment(handWrittenPlan4,"Dept First"))
    println(times)
    println("median: "+median(times))
  */
  }

}
