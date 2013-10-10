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
package org.neo4j.cypher.internal.pipes.optional

// THIS CLASS CAN NOT RUN CONCURRENTLY!
class NullInsertingIterator[ExecutionContext](listener: Listener[ExecutionContext],
                                              inputIterator: Iterator[ExecutionContext],
                                              nullF: ExecutionContext => ExecutionContext)
  extends Iterator[ExecutionContext] {
  var buffer: List[ExecutionContext] = List.empty

  private def fillBuffer(): Boolean = {
    val inputIteratorHasNext = inputIterator.hasNext
    val passedThrough: List[ExecutionContext] = listener.seen.map(nullF)

    if (!inputIteratorHasNext && passedThrough.isEmpty)
      return false

    buffer = if (inputIteratorHasNext) {
      val nextResult = inputIterator.next()
      passedThrough.dropRight(1) :+ nextResult
    }
    else {
      passedThrough
    }

    listener.clear()

    true
  }

  def hasNext: Boolean = {
    if (buffer.isEmpty)
      fillBuffer()

    buffer.nonEmpty
  }

  def next(): ExecutionContext = {
    if (buffer.isEmpty)
      fillBuffer()

    if (buffer.isEmpty)
      Iterator.empty.next() //throws a nice error for us

    val result = buffer.head
    buffer = buffer.tail

    result
  }

  override def toString(): String = "BUFFER: " + buffer.toString
}
