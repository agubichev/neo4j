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
package org.neo4j.cypher.internal.compiler.v2_0.mutation

import org.neo4j.cypher.internal.compiler.v2_0._
import commands.expressions.{Identifier, Expression}
import commands.values.KeyToken
import pipes.QueryState
import symbols._
import org.neo4j.cypher.internal.helpers._
import org.neo4j.graphdb.Node
import collection.Map

object RelationshipEndpoint {
  def apply(name:String) = new RelationshipEndpoint(Identifier(name), NoProperties, Seq.empty, true)
}

case class RelationshipEndpoint(node: Expression, props: PropertyMap, labels: Seq[KeyToken], bare: Boolean)
  extends GraphElementPropertyFunctions {
  def rewrite(f: (Expression) => Expression): RelationshipEndpoint =
    RelationshipEndpoint(node.rewrite(f), props.rewrite(f), labels.map(_.typedRewrite[KeyToken](f)), bare)

  def throwIfSymbolsMissing(symbols: SymbolTable) {
    props.throwIfSymbolsMissing(symbols)
    labels.foreach(_.throwIfSymbolsMissing(symbols))
  }

  def symbolTableDependencies: Set[String] = {
    val nodeDeps = node match {
      case _: Identifier => Set[String]()
      case e => e.symbolTableDependencies
    }
    nodeDeps ++ props.symbolTableDependencies ++ labels.flatMap(_.symbolTableDependencies)
  }
}

case class CreateRelationship(key: String,
                              from: RelationshipEndpoint,
                              to: RelationshipEndpoint,
                              typ: String,
                              props: PropertyMap)
extends UpdateAction
  with GraphElementPropertyFunctions {

  override def children =
    props.children ++ Seq(from.node, to.node) ++
      from.props.children ++ to.props.children ++ to.labels.flatMap(_.children) ++ from.labels.flatMap(_.children)

  override def rewrite(f: (Expression) => Expression) = {
    CreateRelationship(key, from.rewrite(f), to.rewrite(f), typ, props.rewrite(f))
  }

  def exec(context: ExecutionContext, state: QueryState) = {
    val fromVal = from.node(context)(state)
    val toVal = to.node(context)(state)

    val f = fromVal.asInstanceOf[Node]
    val t = toVal.asInstanceOf[Node]
    val relationship = state.query.createRelationship(f, t, typ)
    setProperties(relationship, props, context, state)
    context.put(key, relationship)
    Iterator(context)
  }

  def identifiers = Seq(key-> RelationshipType())

  override def throwIfSymbolsMissing(symbols: SymbolTable) {
    from.throwIfSymbolsMissing(symbols)
    to.throwIfSymbolsMissing(symbols)
    props.throwIfSymbolsMissing(symbols)
  }

  override def symbolTableDependencies: Set[String] = {
      val a = props.symbolTableDependencies
      val b = from.symbolTableDependencies
      val c = to.symbolTableDependencies
      a ++ b ++ c
    }
}
