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
package org.neo4j.cypher.internal.compiler.v2_0

import commands._
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.{Property, Expression, Identifier}
import commands.values.KeyToken
import mutation.GraphElementPropertyFunctions
import symbols._
import org.neo4j.cypher.{CypherTypeException, InternalException, SyntaxException}
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_0.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_0.helpers.IsMap
import org.neo4j.cypher.internal.compiler.v2_0.commands.values.TokenType.PropertyKey

abstract sealed class AbstractPattern extends AstNode[AbstractPattern] {
  def makeOutgoing: AbstractPattern

  def parsedEntities: Seq[ParsedEntity]

  def possibleStartPoints: Seq[(String, CypherType)]

  def name: String

  def start: AbstractPattern
}


object PatternWithEnds {
  def unapply(p: AbstractPattern): Option[(ParsedEntity, ParsedEntity, Seq[String], Direction, Boolean, Option[Int], Option[String])] = p match {
    case ParsedVarLengthRelation(name, _, start, end, typ, dir, optional, None, maxHops, relIterator) => Some((start, end, typ, dir, optional, maxHops, relIterator))
    case ParsedVarLengthRelation(_, _, _, _, _, _, _, Some(x), _, _)                                  => throw new SyntaxException("Shortest path does not support a minimal length")
    case ParsedRelation(name, _, start, end, typ, dir, optional)                                      => Some((start, end, typ, dir, optional, Some(1), Some(name)))
    case _                                                                                            => None
  }
}

object ParsedEntity {
  def apply(name: String) = new ParsedEntity(name, Identifier(name), NoProperties, Seq.empty, true)
}

trait PropertyMap extends TypeSafe {
  def children: Seq[AstNode[_]]

  def rewrite(f: Expression => Expression): PropertyMap

  def isEmpty: Boolean

  def nonEmpty = !isEmpty

  def foreach(f: (String, Any) => Unit, context: ExecutionContext)(implicit state: QueryState)

  def throwIfSymbolsMissing(symbols: SymbolTable)

  def symbolTableDependencies: Set[String]

  def asPredicatesOn(identifier:String): Seq[Predicate]

  def map[T](f: (String, Expression) => T): Seq[T]

  def apply(ctx: ExecutionContext)(implicit state: QueryState): collection.Map[String, Any]
}

case object NoProperties extends PropertyMap {
  def children: Seq[AstNode[_]] = Seq.empty

  def rewrite(f: Expression => Expression): PropertyMap = NoProperties

  def isEmpty = true

  def foreach(f: (String, Any) => Unit, context: ExecutionContext)(implicit state: QueryState) {}

  def throwIfSymbolsMissing(symbols: SymbolTable) {}

  def symbolTableDependencies: Set[String] = Set.empty

  def asPredicatesOn(identifier:String) = Seq.empty

  def map[T](f: (String, Expression) => T): Seq[T] = Seq.empty

  def apply(ctx: ExecutionContext)(implicit state: QueryState) = Map.empty
}

object ExpressionMap {
  def apply(kv: (String, Expression)*) = new ExpressionMap(kv.toMap)
}

case class ExpressionMap(m: Map[String, Expression]) extends PropertyMap {
  def children: Seq[AstNode[_]] = m.values.toSeq

  def rewrite(f: Expression => Expression): PropertyMap = ExpressionMap(m.mapValues(f))

  def isEmpty = m.isEmpty

  def foreach(f: (String, Any) => Unit, context: ExecutionContext)(implicit state: QueryState) {
    m.foreach {
      case (key, expr) => f(key, expr(context))
    }
  }

  def throwIfSymbolsMissing(symbols: SymbolTable) {
    m.values.foreach(_.throwIfSymbolsMissing(symbols))
  }

  def symbolTableDependencies: Set[String] = m.values.map(_.symbolTableDependencies).flatten.toSet

  def asPredicatesOn(identifier: String) = m.map {
    case (propertyKey, expression) => Equals(Property(Identifier(identifier), PropertyKey(propertyKey)), expression)
  }.toSeq

  def map[T](f: (String, Expression) => T): Seq[T] = m.map(kv => f(kv._1, kv._2)).toSeq

  def apply(ctx: ExecutionContext)(implicit state: QueryState) = m.map {
    case (key,expression) => key -> expression(ctx)
  }
}

case class SingleExpressionMap(e: Expression) extends PropertyMap {
  def children: Seq[AstNode[_]] = Seq(e)

  def rewrite(f: Expression => Expression): PropertyMap = SingleExpressionMap(e.rewrite(f))

  def isEmpty = false

  def foreach(f: (String, Any) => Unit, context: ExecutionContext)(implicit state: QueryState): Unit = {
    e(context) match {
      case IsMap(m) => m(state.query).foreach {
        case (key, value) => f(key, value)
      }
    }
  }

  def throwIfSymbolsMissing(symbols: SymbolTable) {
    e.throwIfSymbolsMissing(symbols)
  }

  def symbolTableDependencies: Set[String] = e.symbolTableDependencies

  def asPredicatesOn(identifier: String): Seq[Predicate] = ???

  def map[T](f: (String, Expression) => T): Seq[T] = throw new InternalException("Single expression property map cannot be mapped")

  def apply(ctx: ExecutionContext)(implicit state: QueryState) = e(ctx) match {
    case IsMap(m) => m(state.query)
    case x        => throw new CypherTypeException("Expected to find a map, but got: " + x)
  }
}

case class ParsedEntity(name: String,
                        expression: Expression,
                        props: PropertyMap,
                        labels: Seq[KeyToken],
                        bare: Boolean) extends AbstractPattern with GraphElementPropertyFunctions {
  def makeOutgoing = this

  def parsedEntities = Seq(this)

  def children: Seq[AstNode[_]] = Seq(expression) ++ props.children

  def rewrite(f: (Expression) => Expression) =
    copy(expression = expression.rewrite(f), props = props.rewrite(f), labels = labels.map(_.rewrite(f)))

  def possibleStartPoints: Seq[(String, CypherType)] = Seq(name -> NodeType())

  def start: AbstractPattern = this

  def end: AbstractPattern = this

  def asSingleNode = new SingleNode(name, labels)
}

object ParsedRelation {
  def apply(name: String, start: String, end: String, typ: Seq[String], dir: Direction): ParsedRelation =
    new ParsedRelation(name, NoProperties, ParsedEntity(start), ParsedEntity(end), typ, dir, false)
}

abstract class PatternWithPathName(val pathName: String) extends AbstractPattern {
  def rename(newName: String): PatternWithPathName
}

case class ParsedRelation(name: String,
                          props: PropertyMap,
                          start: ParsedEntity,
                          end: ParsedEntity,
                          typ: Seq[String],
                          dir: Direction,
                          optional: Boolean) extends PatternWithPathName(name)
with Turnable
with GraphElementPropertyFunctions {
  def rename(newName: String): PatternWithPathName = copy(name = newName)

  def turn(start: ParsedEntity, end: ParsedEntity, dir: Direction): AbstractPattern =
    copy(start = start, end = end, dir = dir)

  def parsedEntities = Seq(start, end)

  def children: Seq[AstNode[_]] = Seq(start, end) ++ props.children

  def rewrite(f: (Expression) => Expression) =
    copy(props = props.rewrite(f), start = start.rewrite(f), end = end.rewrite(f))

  def possibleStartPoints: Seq[(String, CypherType)] =
    (start.possibleStartPoints :+ name -> RelationshipType()) ++ end.possibleStartPoints
}

trait Turnable {
  def turn(start: ParsedEntity, end: ParsedEntity, dir: Direction): AbstractPattern

  // It's easier on everything if all relationships are either outgoing or both, but never incoming.
  // So we turn all patterns around, facing the same way
  def dir: Direction

  def start: ParsedEntity

  def end: ParsedEntity

  def makeOutgoing: AbstractPattern = {
    dir match {
      case Direction.INCOMING => turn(start = end, end = start, dir = Direction.OUTGOING)
      case Direction.OUTGOING => this.asInstanceOf[AbstractPattern]
      case Direction.BOTH     => (start.expression, end.expression) match {
        case (Identifier(a), Identifier(b)) if a < b  => this.asInstanceOf[AbstractPattern]
        case (Identifier(a), Identifier(b)) if a >= b => turn(start = end, end = start, dir = dir)
        case _                                        => this.asInstanceOf[AbstractPattern]
      }
    }
  }

}


case class ParsedVarLengthRelation(name: String,
                                   props: PropertyMap,
                                   start: ParsedEntity,
                                   end: ParsedEntity,
                                   typ: Seq[String],
                                   dir: Direction,
                                   optional: Boolean,
                                   minHops: Option[Int],
                                   maxHops: Option[Int],
                                   relIterator: Option[String])
  extends PatternWithPathName(name)
  with Turnable
  with GraphElementPropertyFunctions {
  def rename(newName: String): PatternWithPathName = copy(name = newName)

  def turn(start: ParsedEntity, end: ParsedEntity, dir: Direction): AbstractPattern =
    copy(start = start, end = end, dir = dir)

  def parsedEntities = Seq(start, end)

  def children: Seq[AstNode[_]] = Seq(start, end) ++ props.children

  def rewrite(f: (Expression) => Expression) =
    copy(props = props.rewrite(f), start = start.rewrite(f), end = end.rewrite(f))

  def possibleStartPoints: Seq[(String, CypherType)] =
    (start.possibleStartPoints :+ name -> CollectionType(RelationshipType())) ++ end.possibleStartPoints
}

case class ParsedShortestPath(name: String,
                              props: PropertyMap,
                              start: ParsedEntity,
                              end: ParsedEntity,
                              typ: Seq[String],
                              dir: Direction,
                              optional: Boolean,
                              maxDepth: Option[Int],
                              single: Boolean,
                              relIterator: Option[String])
  extends PatternWithPathName(name) with GraphElementPropertyFunctions {
  def rename(newName: String): PatternWithPathName = copy(name = newName)

  def makeOutgoing = this

  def parsedEntities = Seq(start, end)

  def children: Seq[AstNode[_]] = Seq(start, end) ++ props.children

  def rewrite(f: (Expression) => Expression) =
    copy(props = props.rewrite(f), start = start.rewrite(f), end = end.rewrite(f))

  def possibleStartPoints: Seq[(String, CypherType)] =
    (start.possibleStartPoints :+ name -> PathType()) ++ end.possibleStartPoints
}

case class ParsedNamedPath(name: String, pieces: Seq[AbstractPattern]) extends PatternWithPathName(name) {

  assert(pieces.nonEmpty)

  def rename(newName: String): PatternWithPathName = copy(name = newName)

  def makeOutgoing = this

  def parsedEntities = pieces.flatMap(_.parsedEntities)

  def children: Seq[AstNode[_]] = pieces

  def rewrite(f: (Expression) => Expression): AbstractPattern = copy(pieces = pieces.map(_.rewrite(f)))

  def possibleStartPoints: Seq[(String, CypherType)] = pieces.flatMap(_.possibleStartPoints)

  def start: AbstractPattern = pieces.head

  def end: AbstractPattern = pieces.last
}

