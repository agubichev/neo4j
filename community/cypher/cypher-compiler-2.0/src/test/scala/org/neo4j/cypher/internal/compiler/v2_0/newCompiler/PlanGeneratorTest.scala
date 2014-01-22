package org.neo4j.cypher.internal.compiler.v2_0.newCompiler

import org.scalatest.Assertions
import org.junit.Test
import org.scalatest.mock.MockitoSugar
import org.neo4j.cypher.internal.compiler.v2_0.spi.PlanContext
import org.mockito.Mockito._
import org.neo4j.graphdb.Direction


class PlanGeneratorTest extends Assertions with MockitoSugar {

  val planContext = mock[PlanContext]
  val costEstimator = mock[CostEstimator]
  val generator = new PlanGenerator(costEstimator)
  
  @Test def simplePattern() {
    // MATCH (a) RETURN a
    // GIVEN
    val queryGraph = QueryGraph(Id(0), Seq.empty, Seq.empty, Seq.empty)
    when(costEstimator.costForAllNodes()).thenReturn(Cost(1000, 1))

    // WHEN
    val result = generator.generatePlan(planContext, queryGraph)

    // THEN
    assert(AllNodesScan(Id(0), Cost(1000, 1)) === result)
  }

  @Test def simpleLabeledPattern() {
    // MATCH (a:Person) RETURN a
    // Given
    val personLabelId: Int = 1337
    when(planContext.getLabelId("Person")).thenReturn(personLabelId)
    val queryGraph = QueryGraph(Id(0), Seq.empty, Seq(Id(0) -> NodeLabelSelection(Label("Person"))), Seq.empty)
    when(costEstimator.costForScan(Token(personLabelId))).thenReturn(Cost(100, 1))

    // When
    val result = generator.generatePlan(planContext, queryGraph)

    // Then
    assert(LabelScan(Id(0), Token(personLabelId), Cost(100, 1)) === result)
  }

  @Test def simpleRelationshipPatternWithCheaperLabelScan() {
    // MATCH (a:Person) -[:KNOWS]-> (b) RETURN a
    // Given

    val personLabelId: Int = 1337
    val knowsTypeId: Int = 12

    when(planContext.getLabelId("Person")).thenReturn(personLabelId)
    when(planContext.getRelationshipTypeId("KNOWS")).thenReturn(knowsTypeId)
    val queryGraph = QueryGraph(Id(1), Seq(
      GraphRelationship(Id(0), Id(1), Direction.OUTGOING, Seq(RelationshipType("KNOWS")))
    ), Seq(Id(0) -> NodeLabelSelection(Label("Person"))), Seq.empty)
    when(costEstimator.costForAllNodes()).thenReturn(Cost(1000, 1))
    when(costEstimator.costForScan(Token(personLabelId))).thenReturn(Cost(100, 1))
    when(costEstimator.costForExpandRelationship(Seq.empty, Seq(Token(knowsTypeId)), Direction.OUTGOING)).thenReturn(Cost(500, 1))

    // When
    val result = generator.generatePlan(planContext, queryGraph)

    // Then
    assert(ExpandRelationships(LabelScan(Id(0), Token(personLabelId), Cost(100, 1)), Direction.OUTGOING, Cost(500, 1)) === result)
  }
}
