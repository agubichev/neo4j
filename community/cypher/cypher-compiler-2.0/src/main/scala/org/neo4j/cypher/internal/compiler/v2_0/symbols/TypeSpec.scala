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
package org.neo4j.cypher.internal.compiler.v2_0.symbols

object TypeSpec {
  def exact(types: CypherType*): TypeSpec = exact(types)
  def exact[T <: CypherType](traversable: TraversableOnce[T]): TypeSpec = TypeSpec(traversable.map(t => TypeRange(t, t)))
  def all: TypeSpec = TypeSpec(TypeRange(CTAny, None))
  val none: TypeSpec = new TypeSpec(Vector.empty)

  private val simpleTypes = Vector(
    CTAny,
    CTBoolean,
    CTDouble,
    CTInteger,
    CTLong,
    CTMap,
    CTNode,
    CTNumber,
    CTPath,
    CTRelationship,
    CTString
  )

  private def apply(range: TypeRange): TypeSpec = new TypeSpec(Vector(range))
  private def apply(ranges: TraversableOnce[TypeRange]): TypeSpec = new TypeSpec(ranges.foldLeft(Vector.empty[TypeRange]) {
    case (set, range) =>
      if (set.exists(_ contains range))
        set
      else
        set.filterNot(range contains) :+ range
  })
}

class TypeSpec private (private val ranges: Seq[TypeRange]) extends Equals {
  def contains(that: CypherType): Boolean = contains(that, ranges)
  private def contains(that: CypherType, rs: Seq[TypeRange]): Boolean = rs.exists(_ contains that)

  def containsAny(types: CypherType*): Boolean = containsAny(TypeSpec.exact(types:_*))
  def containsAny(that: TypeSpec): Boolean = ranges.exists {
    r1 => that.ranges.exists(r2 => (r1 constrain r2.lower).isDefined)
  }

  def union(that: TypeSpec): TypeSpec = TypeSpec(ranges ++ that.ranges)
  def |(that: TypeSpec): TypeSpec = union(that)

  def =:=(that: CypherType): TypeSpec = intersect(TypeSpec.exact(that))
  def intersect(that: TypeSpec): TypeSpec = TypeSpec(ranges.flatMap {
    r => that.ranges.flatMap(r intersect)
  })
  def &(that: TypeSpec): TypeSpec = intersect(that)

  def <:<(that: CypherType): TypeSpec = constrain(TypeSpec.exact(that))
  def constrain(that: TypeSpec): TypeSpec = TypeSpec(ranges.flatMap {
    r => that.ranges.flatMap(r constrain _.lower)
  })

  def >:>(that: CypherType): TypeSpec = mergeUp(TypeSpec.exact(that))
  def mergeUp(that: TypeSpec): TypeSpec = TypeSpec(ranges.flatMap {
    r => that.ranges.flatMap(r mergeUp)
  })

  def reparent(f: CypherType => CypherType): TypeSpec = TypeSpec(ranges.map(_.reparent(f)))

  def isEmpty: Boolean = ranges.isEmpty
  def nonEmpty: Boolean = !isEmpty

  def hasDefiniteSize: Boolean = _hasDefiniteSize
  private lazy val _hasDefiniteSize = ranges.forall(_.hasDefiniteSize)

  def toStream: Stream[CypherType] = toStream(ranges)
  private def toStream(rs: => Seq[TypeRange]): Stream[CypherType] =
    if (rs.isEmpty)
      Stream()
    else
      TypeSpec.simpleTypes.filter(contains(_, rs)).toStream append toStream(innerTypeRanges(rs)).map(t => CollectionType(t))

  def iterator: Iterator[CypherType] = toStream.iterator

  override def hashCode = 41 * ranges.hashCode
  override def equals(that: Any): Boolean = that match {
    case that: TypeSpec =>
      (that canEqual this) && {
        val (finite1, infinite1) = ranges.partition(_.hasDefiniteSize)
        val (finite2, infinite2) = that.ranges.partition(_.hasDefiniteSize)
        (infinite1 == infinite2) &&
        ((finite1 == finite2) || (toStream(finite1) == toStream(finite2)))
      }
    case _              => false
  }
  override def canEqual(that: Any): Boolean = that.isInstanceOf[TypeSpec]

  def toStrings: IndexedSeq[String] = toStrings(Vector.empty, ranges, identity)
  private def toStrings(acc: IndexedSeq[String], rs: Seq[TypeRange], format: String => String): IndexedSeq[String] =
    if (rs.isEmpty)
      acc
    else if (rs.exists({ case TypeRange(_: AnyType, None) => true case _ => false }))
      acc :+ format("T")
    else
      toStrings(acc ++ TypeSpec.simpleTypes.filter(contains(_, rs)).map(t => format(t.toString)), innerTypeRanges(rs), t => s"Collection<${format(t)}>")

  def mkString(sep: String): String =
    mkString("", sep, sep, "")
  def mkString(sep: String, lastSep: String): String =
    mkString("", sep, lastSep, "")
  def mkString(start: String, sep: String, end: String): String =
    mkString(start, sep, sep, end)
  def mkString(start: String, sep: String, lastSep: String, end: String): String =
    addString(new StringBuilder(), start, sep, lastSep, end).toString()

  def addString(b: StringBuilder, start: String, sep: String, lastSep: String, end: String): StringBuilder = {
    val strings = toStrings
    if (strings.length > 1)
      strings.dropRight(1).addString(b, start, sep, "").append(lastSep).append(strings.last).append(end)
    else
      strings.addString(b, start, sep, end)
  }

  override def toString = mkString("TypeSet(", ", ", ")")

  private def innerTypeRanges(rs: Seq[TypeRange]): Seq[TypeRange] = rs.flatMap {
    case TypeRange(c: CollectionType, Some(u: CollectionType)) => Some(TypeRange(c.innerType, u.innerType))
    case TypeRange(c: CollectionType, None)                    => Some(TypeRange(c.innerType, None))
    case TypeRange(_: AnyType, Some(u: CollectionType))        => Some(TypeRange(CTAny, u.innerType))
    case r@TypeRange(_: AnyType, None)                         => Some(r)
    case _                                                     => None
  }
}
