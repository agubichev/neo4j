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
package org.neo4j.cypher.internal.compiler.v2_0.commands

import expressions._
import values.KeyToken
import values.TokenType.PropertyKey
import org.neo4j.cypher.internal.compiler.v2_0._
import org.neo4j.cypher.internal.compiler.v2_0.mutation.{UpdateAction, PropertySetAction, MergeSingleNodeAction}
import org.neo4j.cypher.PatternException
import scala.collection.mutable
import org.neo4j.graphdb.Direction
import scala.collection

case class MergeAst(patterns: Seq[AbstractPattern], onActions: Seq[OnAction]) {
  def nextStep(): Seq[UpdateAction] = {

    val actionsMap = new mutable.HashMap[(String, Action), mutable.Set[UpdateAction]] with mutable.MultiMap[(String, Action), UpdateAction]

    for (
      actions <- onActions;
      action <- actions.set) {
      actionsMap.addBinding(actions.identifier -> actions.verb, action)
    }

    def getActions(name: String, action: Action): Seq[UpdateAction] =
      actionsMap.get(name -> action).getOrElse(Set.empty).toSeq

    patterns.map {
      case ParsedEntity(name, _, props, labelTokens, _) =>

        val labelPredicates = labelTokens.map(labelName => HasLabel(Identifier(name), labelName))
        val (propertyPredicates, propertyMap, propertyActions) = mangleProperties(props, name)
        val predicates = labelPredicates ++ propertyPredicates
        val labelActions = labelTokens.map(labelName => LabelAction(Identifier(name), LabelSetOp, Seq(labelName)))

        val onCreate: Seq[UpdateAction] = labelActions ++ propertyActions ++ getActions(name, On.Create)

        MergeSingleNodeAction(
          identifier = name,
          props = propertyMap,
          labels = labelTokens,
          expectations = predicates,
          onCreate = onCreate,
          onMatch = getActions(name, On.Match),
          maybeNodeProducer = None)

      case ParsedRelation(name, props, firstEntity, secondEntity, types, dir, false) =>

        val (start, end) = if (dir == Direction.OUTGOING) (firstEntity.name, secondEntity.name)
        else (secondEntity.name, firstEntity.name)

        val (propertyPredicates, _, propertyActions) = mangleProperties(props, name)

          ???
//        MergeRelationshipsAction(
//          startNodeIdentifier = start,
//          endNodeIdentifier = end,
//          identifier = name,
//          relType = types.head,
//          expectations = propertyPredicates.toSeq,
//          onCreate = propertyActions ++ getActions(name, On.Create),
//          onMatch = getActions(name, On.Match))

      case _ =>
        throw new PatternException("MERGE only supports single node patterns or relationships")
    }
  }

  private def mangleProperties(props: collection.Map[String, Expression], name: String):
  (Seq[Predicate], Map[KeyToken, Expression], Seq[UpdateAction]) = {
    val propertyPredicates = props.map {
      case (propertyKey, expression) => Equals(Property(Identifier(name), PropertyKey(propertyKey)), expression)
    }

    val propertyMap: Map[KeyToken, Expression] = props.map {
      case (propertyKey, expression) => PropertyKey(propertyKey) -> expression
    }.toMap

    val propertyActions = props.map {
      case (propertyKey, expression) =>
        if (propertyKey == "*") throw new PatternException("MERGE does not support map parameters")
        PropertySetAction(Property(Identifier(name), PropertyKey(propertyKey)), expression)
    }

    (propertyPredicates.toSeq, propertyMap, propertyActions.toSeq)
  }
}


