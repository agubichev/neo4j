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
import commands.{expressions => commandexpressions}
import symbols._

case object Subtract extends ArithmeticInfixFunction {
  val name = "-"

  override def semanticCheck(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation): SemanticCheck =
    checkMinArgs(invocation, 1) then checkMaxArgs(invocation, 2) then
    when(invocation.arguments.length == 1) {
      semanticCheckUnary(ctx, invocation)
    } then when(invocation.arguments.length == 2) {
      semanticCheckInfix(ctx, invocation)
    }

  private def semanticCheckUnary(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation): SemanticCheck =
    invocation.arguments.expectType(T <:< CTInteger | T <:< CTLong | T <:< CTDouble) then
    invocation.specifyType(invocation.arguments(0).types)

  private def semanticCheckInfix(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation): SemanticCheck =
    checkArgs(invocation, 2) ifOkThen {
      invocation.arguments.expectType(T <:< CTInteger | T <:< CTLong | T <:< CTDouble) then
      invocation.specifyType(infixOutputTypes(invocation.arguments(0), invocation.arguments(1)))
    }

  def toCommand(invocation: ast.FunctionInvocation) =
    if (invocation.arguments.length == 1) {
      commandexpressions.Subtract(commandexpressions.Literal(0), invocation.arguments(0).toCommand)
    } else {
      val left = invocation.arguments(0)
      val right = invocation.arguments(1)
      commandexpressions.Subtract(left.toCommand, right.toCommand)
    }
}
