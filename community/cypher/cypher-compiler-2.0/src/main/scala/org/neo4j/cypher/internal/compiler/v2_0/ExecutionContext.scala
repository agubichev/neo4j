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
import scala.collection

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

class MapExecutionContext(val m: MutableMap[String, Any] = MutableMaps.empty) extends ExecutionContext {

  def slots: Set[Slot] = {
    val slotSet: collection.Set[String] = m.keySet
    slotSet.asInstanceOf[Set[String]]
  }

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
}



//case class ExecutionContext(m: MutableMap[String, Any] = MutableMaps.empty,
//                            mutationCommands: Queue[UpdateAction] = Queue.empty)
//  extends MutableMap[String, Any] {
//  def get(key: String): Option[Any] = m.get(key)
//
//  def iterator: Iterator[(String, Any)] = m.iterator
//
//  override def size = m.size
//
//  def ++(other: ExecutionContext): ExecutionContext = copy(m = m ++ other.m)
//
//  override def foreach[U](f: ((String, Any)) => U) {
//    m.foreach(f)
//  }
//
//  def +=(kv: (String, Any)) = {
//    m += kv
//    this
//  }
//
//  def -=(key: String) = {
//    m -= key
//    this
//  }
//
//  override def toMap[T, U](implicit ev: (String, Any) <:< (T, U)): immutable.Map[T, U] = m.toMap(ev)
//
//  def newWith(newEntries: Seq[(String, Any)]) =
//    createWithNewMap(MutableMaps.create(this.m) ++= newEntries)
//
//  def newWith(newEntries: scala.collection.Map[String, Any]) =
//    createWithNewMap(MutableMaps.create(this.m) ++= newEntries)
//
//  def newFrom(newEntries: Seq[(String, Any)]) =
//    createWithNewMap(MutableMaps.create(newEntries: _*))
//
//  def newFrom(newEntries: scala.collection.Map[String, Any]) =
//    createWithNewMap(MutableMaps.create(newEntries))
//
//  def newWith(newEntry: (String, Any)) =
//    createWithNewMap(MutableMaps.create(this.m) += newEntry)
//
//  override def clone(): ExecutionContext = newFrom(m)
//
//  protected def createWithNewMap(newMap: MutableMap[String, Any]) = {
//    copy(m = newMap)
//  }
//}
//
