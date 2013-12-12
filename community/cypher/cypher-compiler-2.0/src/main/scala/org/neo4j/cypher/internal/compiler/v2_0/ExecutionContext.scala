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

object ExecutionContext {
  def empty = new ArrayExecutionContext()
  def from(in: (String, Any)*) = ExecutionContext.empty.update(in)
  def from(in: Iterable[(String, Any)]) = ExecutionContext.empty.update(in)
}

abstract class ExecutionContext {
  def slots: Set[String]

  def get(slot: String): Option[Any]

  def getOrElse(slot: String, f: => Any): Any

  def contains(slot: String): Boolean

  def containsAll(slots: Seq[String]): Boolean = slots.isEmpty || slots.forall(contains)

  def apply(slot: String): Any

  def apply(index: Int): Any

  def collect[T](f: PartialFunction[(String, Any), T]): Seq[T]

  def collectValues[T](f: PartialFunction[Any, T]): Seq[T]

  def update(slot: String, value: Any): ExecutionContext

  def update(input: Iterable[(String, Any)]): ExecutionContext = {
    input foreach update
    this
  }

  def update(kv: (String, Any)): ExecutionContext = kv match {
    case (key, value) => update(key, value)
  }

  def update(m: Map[String, Any]): ExecutionContext = {
    m.foreach(update)
    this
  }

  def copy(): ExecutionContext

  def toMap: Map[String, Any]
}

object ArrayExecutionContext {
  case object EMPTY
}

class ArrayExecutionContext private(private var data: Array[Any], private var keys: Seq[String]) extends ExecutionContext {

  import ArrayExecutionContext.EMPTY

  def this() = this(Array(), Seq())

  def this(keys: Seq[String]) = {
    this(null, keys)
    assert(keys == keys.distinct)
    data = Array.fill(keys.size)(EMPTY)
  }

  def slots: Set[String] = keys.toSet

  def contains(slot: String): Boolean = get(slot).nonEmpty

  def apply(index: Int): Any = data(index)

  def get(slot: String): Option[Any] = indexOf(slot).map(data(_)) match {
    case Some(EMPTY) => None
    case x => x
  }

  def getOrElse(slot: String, f: => Any): Any = get(slot).getOrElse(f)

  def apply(slot: String): Any = get(slot).
    getOrElse(throw new NoSuchElementException("The " + slot + " was not found"))

  def collect[T](f: PartialFunction[(String, Any), T]): Seq[T] =
    (keys zip data).collect(f)

  def collectValues[T](f: PartialFunction[Any, T]): Seq[T] =
    data.collect(f)

  def update(slot: String, value: Any): ExecutionContext = {
    indexOf(slot) match {
      case Some(idx) => data(idx) = value
      case None      => extendArrayAndSet(slot, value)
    }

    this
  }

  def copy(): ExecutionContext =
    new ArrayExecutionContext(data.clone(), keys)

  def toMap: Map[String, Any] = (keys zip data).toMap.filter {
    case (_, EMPTY) => false
    case _          => true
  }

  override def toString: String = {
    val data = collect {
      case (slot, value) => slot + " -> " + value.toString
    }.mkString(", ")

    "ExecutionContext( " + data + " )"
  }

  override def equals(obj: Any): Boolean = obj match {
    case m: Map[_, _] => toMap.equals(m)
    case other: ExecutionContext => toMap.equals(other.toMap)
    case _ => false
  }

  private def extendArrayAndSet(slot: String, value: Any) = {
    keys = keys :+ slot
    val oldData = data
    data = new Array[Any](oldData.length + 1)
    oldData.copyToArray(data)
    data(oldData.length) = value
  }

  private def indexOf(key: String): Option[Int] = {
    val idx = keys.indexOf(key)

    if (idx < 0) None else Some(idx)
  }
}
