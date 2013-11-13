package org.neo4j.cypher.internal.compiler.v2_0.mutation

import org.scalatest.Assertions
import org.neo4j.cypher.internal.compiler.v2_0.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_0.pipes.QueryStateHelper
import org.junit.Test

class MergeLinkTest extends Assertions {

  implicit val qs = QueryStateHelper.empty

  @Test
  def shouldDropThingLinks() {
    // given
    val link1: MergeLink = link("a", "b", "c")
    val link2: MergeLink = link("a", "j", "k")

    val links = Set(link1, link2)

    // when
    val (newLinks, Some((nextLink, nextStep))) =
      MergeLink.select(links, MergeStep.DoNothing.priority)(context("a", "b", "c"))

    // then
    assert( Set(link2) === newLinks )
    assert( link1 == nextLink )
    assert( MergeStep.Traverse.priority === nextStep.priority )
  }

  def link(left: String, rel: String, right: String) =
    MergeLink(NodeMergeEntity(left, Seq.empty, Seq.empty, Seq.empty),
              RelationshipMergeEntity(rel, Seq.empty, Seq.empty, Seq.empty), "KNOWS",
              NodeMergeEntity(right, Seq.empty, Seq.empty, Seq.empty))

  def context(identifiers: String*) = ExecutionContext.from( identifiers.map( (id: String) => id -> id ): _* )
}