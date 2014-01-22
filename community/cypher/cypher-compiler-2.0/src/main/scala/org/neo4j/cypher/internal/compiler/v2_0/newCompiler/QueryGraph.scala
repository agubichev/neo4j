package org.neo4j.cypher.internal.compiler.v2_0.newCompiler

import org.neo4j.cypher.internal.compiler.v2_0.ast.Expression
import org.neo4j.graphdb.Direction

case class QueryGraph(maxId: Id,
                      edges: Seq[Edge],
                      selections: Seq[(Id, Selection)],
                      projection: Seq[(Expression, Int)]) {
  // build bitmap of connected nodes
}

trait Edge {
  def start: Id

  def end: Id
}

trait Comparison
case object Equal extends Comparison
case object NotEqual extends Comparison
case object LargerThan extends Comparison
case object LargerThanOrEqual extends Comparison

case class PropertyJoin(start: Id, end: Id, startPropertyId: PropertyKey, endPropertyId: PropertyKey, comparison: Comparison) extends Edge
case class GraphRelationship(start: Id, end: Id, direction: Direction, types: Seq[RelationshipType]) extends Edge

trait Selection
case class NodePropertySelection(property: PropertyKey, value: Option[Any], comparison: Comparison) extends Selection
case class NodeLabelSelection(label: Label) extends Selection

case class Id(id: Int) extends AnyVal
case class PropertyKey(name: String) extends AnyVal
case class RelationshipType(name: String) extends AnyVal
case class Label(name: String) extends AnyVal

