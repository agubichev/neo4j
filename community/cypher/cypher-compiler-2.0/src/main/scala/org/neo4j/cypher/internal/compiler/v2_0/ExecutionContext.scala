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

import pipes.MutableMaps
import collection.mutable.{Map => MutableMap}

object ExecutionContext {
  def empty = new MapExecutionContext()
  def empty(size: Int) = new MapExecutionContext(MutableMaps.create(size))

  def from(in: (String, Any)*) = ExecutionContext.empty.update(in)
  def from(in: Iterable[(String, Any)]) = ExecutionContext.empty.update(in)
}

abstract class ExecutionContext {
  type Slot = String

  def slots: Set[Slot]

  def get(slot: Slot): Option[Any]

  def getOrElse(slot: Slot, f: => Any): Any

  def contains(slot: Slot): Boolean

  def containsAll(slots: Seq[Slot]): Boolean = slots.isEmpty || slots.forall(contains)

  def apply(slot: Slot): Any

  def collect[T](f: PartialFunction[(Slot, Any), T]): Seq[T]

  def collectValues[T](f: PartialFunction[Any, T]): Seq[T]

  def update(slot: Slot, value: Any): ExecutionContext

  def update(input: Iterable[(Slot, Any)]): ExecutionContext = {
    input foreach update
    this
  }

  def update(kv: (Slot, Any)): ExecutionContext = kv match {
    case (key, value) => update(key, value)
  }

  def update(m: Map[Slot, Any]): ExecutionContext = {
    m.foreach(update)
    this
  }

  def -=(slot: Slot): Any

  def copy(): ExecutionContext

  def toMap(): Map[String, Any]
}

case class MapExecutionContext(m: MutableMap[String, Any] = MutableMaps.empty) extends ExecutionContext {

  def slots: Set[Slot] = m.keySet.toSet

  def contains(slot: Slot): Boolean = m.contains(slot)

  def get(slot: MapExecutionContext#Slot): Option[Any] = m.get(slot)

  def getOrElse(slot: Slot, f: => Any): Any = m.getOrElse(slot, f)

  def apply(slot: Slot): Any = m(slot)

  def collect[T](f: PartialFunction[(Slot, Any), T]): Seq[T] = m.collect(f).toSeq

  def collectValues[T](f: PartialFunction[Any, T]): Seq[T] = m.values.collect(f).toSeq

  def update(slot: Slot, value: Any): ExecutionContext = {
    m.put(slot, value)
    this
  }

  def -=(slot: Slot): Any = {
    m -= slot
    this
  }

  def copy(): ExecutionContext = new MapExecutionContext(m.clone())

  def toMap(): Map[String, Any] = m.toMap

  override def toString: String = "ExecutionContext(" + m.mkString(", ") + ")"

  override def equals(obj: Any): Boolean = obj match {
    case m: Map[String, Any]        => m.equals(m)
    case other: MapExecutionContext => m.equals(other.m)
    case _                          => false
  }
}
