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
package org.neo4j.cypher.internal.compiler.v2_0.ast

import org.neo4j.cypher.internal.compiler.v2_0.InputToken

trait AstNode extends Product {
  def token: InputToken

  def rewrite(rewriter: PartialFunction[AstNode, AstNode]) = _rewrite(this)(rewriter)

  private def _rewrite[T <: AstNode](in: Any)(rewriter: PartialFunction[AstNode, AstNode]): Any = {
    def mapProduct(f: Any => Any, p: Product): Product = {
      var newObjects = Seq[AnyRef]()

      p.productIterator foreach {
        originalObject =>
          val newObject = mapFunction(originalObject)
          val rewrittenObject = newObject match {
            case obj: AstNode => _rewrite(obj)(rewriter)
            case _            => newObject
          }

          newObjects = newObjects :+ rewrittenObject.asInstanceOf[AnyRef]
      }

      val constructor = p.getClass.getMethods.find(_.getName == "copy").get
      constructor.invoke(p, newObjects: _*).asInstanceOf[T]
    }

    def mapKeyAndValue(f: Any => Any, m: Map[_, _]): Map[_, _] = m.map {
      case (k, v) =>
        val newK = f(k)
        val newV = f(v)
        newK -> newV
    }

    def mapFunction: Function[Any, Any] = {
      case e: AstNode if rewriter.isDefinedAt(e) => rewriter(e)
      case e: Option[_]                          => e.map(_rewrite(_)(rewriter))
      case m: Map[_, _]                          => mapKeyAndValue(_rewrite(_)(rewriter), m)
      case e: Traversable[_]                     => e.map(_rewrite(_)(rewriter))
      case p: Product                            => mapProduct(_rewrite(_)(rewriter), p)
      case e                                     => e
    }

    mapFunction(in)
  }
}
