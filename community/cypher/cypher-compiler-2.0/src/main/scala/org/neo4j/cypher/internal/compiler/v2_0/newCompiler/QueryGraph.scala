package org.neo4j.cypher.internal.compiler.v2_0.newCompiler

import org.neo4j.cypher.internal.compiler.v2_0.ast.Expression
import org.neo4j.graphdb.Direction
import scala.collection.mutable

case class QueryGraph(maxId: Id,
                      edges: Seq[QueryEdge],
                      selections: Seq[(Id, Selection)],
                      projection: Seq[(Expression, Int)]) {
  // build bitmap of connected nodes

  val selectionsByNode: Map[Id, Seq[Selection]] =
    selections.groupBy(_._1).mapValues(_.map(_._2)).withDefault(_ => Seq.empty)
  
  val edgesByIds: Map[JoinLink, Seq[QueryEdge]] = Map.empty
  
  def optJoinMethods(leftIds: Set[Id], rightIds: Set[Id]): Seq[PropertyJoin] = {
    var joins = Seq.newBuilder[PropertyJoin]
    for (leftId <- leftIds;
         rightId <- rightIds if leftId.id <= rightId.id) {
      val link = new JoinLink(leftId, rightId)
      edgesByIds.get(link) match {
        case Some(propertyJoins) => joins ++= propertyJoins
        case None =>
      }
    }
    joins.result()
  }
}

class JoinLink(left: Id, right: Id) {
  val (start, end) =
    if (left.id <= right.id)
      (left, right)
    else
      (right, left)
}

trait QueryEdge {
  def start: Id
  def end: Id
  
  val link = new JoinLink(start, end)
}

trait Comparison
case object Equal extends Comparison
case object NotEqual extends Comparison
case object LargerThan extends Comparison
case object LargerThanOrEqual extends Comparison

case class PropertyJoin(start: Id, end: Id, startPropertyId: PropertyKey, endPropertyId: PropertyKey, comparison: Comparison) extends QueryEdge
case class GraphRelationship(start: Id, end: Id, direction: Direction, types: Seq[RelationshipType]) extends QueryEdge

trait Selection
case class NodePropertySelection(property: PropertyKey, value: Option[Any], comparison: Comparison) extends Selection
case class NodeLabelSelection(label: Label) extends Selection

case class Id(id: Int) extends AnyVal {
  def allIncluding: Seq[Id] = 0.to(id).map(Id(_))
}

case class PropertyKey(name: String) extends AnyVal
case class RelationshipType(name: String) extends AnyVal
case class Label(name: String) extends AnyVal

case class Token(value: Int) extends AnyVal
