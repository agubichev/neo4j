/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v2_0.pipes.matching

import org.neo4j.cypher.internal.compiler.v2_0._
import commands._
import pipes.QueryState
import org.neo4j.cypher.internal.helpers._
import org.neo4j.graphdb.{Node, Relationship, Direction}

object SingleStep {
  def apply(id: Int, typ: Seq[String], direction: Direction, next: Option[ExpanderStep], relPredicate: Predicate, nodePredicate: Predicate) =
    new SingleStep(id, typ, direction, next, relPredicate, nodePredicate, "r" + id, "n" + id)
}

case class SingleStep(id: Int,
                      typ: Seq[String],
                      direction: Direction,
                      next: Option[ExpanderStep],
                      relPredicate: Predicate,
                      nodePredicate: Predicate,
                      relName: String,
                      nodeName: String) extends ExpanderStep {

  def createCopy(next: Option[ExpanderStep], direction: Direction, nodePredicate: Predicate): ExpanderStep =
    copy(next = next, direction = direction, nodePredicate = nodePredicate)

  private val combinedPredicate: Predicate = And(relPredicate, nodePredicate)
  private val needToFilter = combinedPredicate != True()

  def expand(node: Node, parameters: ExecutionContext, state: QueryState): (Iterable[Relationship], Option[ExpanderStep]) = {
    val rels = DynamicIterable {
      val allRelationships = state.query.getRelationshipsFor(node, direction, typ)

      if (needToFilter)
        allRelationships.filter {
          r =>
            val otherNode = r.getOtherNode(node)
            parameters.update(relName, r)
            parameters.update(nodeName, otherNode)
            combinedPredicate.isTrue(parameters)(state)
        }
      else
        allRelationships
    }
    (rels, next)
  }

  override def toString = {
    val left =
      if (direction == Direction.OUTGOING)
        ""
      else
        "<"

    val right =
      if (direction == Direction.INCOMING)
        ""
      else
        ">"

    val relInfo = typ.toList match {
      case List() => "[{%s,%s}]".format(relPredicate, nodePredicate)
      case _      => "[:%s {%s,%s}]".format(typ.mkString("|"), relPredicate, nodePredicate)
    }

    val shape = "(%s)%s-%s-%s".format(id, left, relInfo, right)

    next match {
      case None    => "%s()".format(shape)
      case Some(x) => shape + x.toString
    }
  }

  def size: Option[Int] = next match {
    case None    => Some(1)
    case Some(n) => n.size.map(_ + 1)
  }

  override def equals(p1: Any) = p1 match {
    case null                => false
    case other: ExpanderStep =>
      val a = id == other.id
      val b = direction == other.direction
      val c = next == other.next
      val d = typ == other.typ
      val e = relPredicate == other.relPredicate
      val f = nodePredicate == other.nodePredicate
      a && b && c && d && e && f
    case _                   => false
  }

  def shouldInclude() = false
}
