/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.functions

import org.neo4j.cypher.internal.compiler.v2_0._
import symbols._

case object LessThanOrEqual extends PredicateFunction {
  def name = "<="

  def semanticCheck(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation): SemanticCheck =
    checkArgs(invocation, 2) ifOkThen {
      val lhs = invocation.arguments(0)
      val rhs = invocation.arguments(1)

      lhs.expectType(T <:< CTInteger | T <:< CTLong | T <:< CTDouble | T <:< CTString) then
      rhs.expectType(lhs.types)
    } then invocation.specifyType(CTBoolean)

  protected def internalToPredicate(invocation: ast.FunctionInvocation) = {
    val left = invocation.arguments(0)
    val right = invocation.arguments(1)
    commands.LessThanOrEqual(left.toCommand, right.toCommand)
  }
}
