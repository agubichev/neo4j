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

import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions

class TypeSpecTest extends Assertions {

  @Test
  def allTypesShouldContainAll() {
    assertTrue(T contains CTAny)
    assertTrue(T contains CTString)
    assertTrue(T contains CTNumber)
    assertTrue(T contains CTInteger)
    assertTrue(T contains CTDouble)
    assertTrue(T contains CTNode)
    assertTrue(T contains CTCollectionAny)
    assertTrue(T contains CTCollection(CTDouble))
    assertTrue(T contains CTCollection(CTCollection(CTDouble)))
  }

  @Test
  def shouldReturnTrueIfContainsAny() {
    assertTrue(T.containsAny(T))
    assertTrue(T.containsAny(T <:< CTNumber))
    assertTrue(T.containsAny(TypeSpec.exact(CTNode)))
    assertTrue((T <:< CTNumber).containsAny(T))

    assertTrue((T <:< CTNumber).containsAny(CTInteger))
    assertTrue((T <:< CTInteger).containsAny(CTInteger, CTString))
    assertTrue((T <:< CTInteger).containsAny(T <:< CTNumber))
    assertTrue((T <:< CTInteger).containsAny(T))

    assertFalse((T <:< CTInteger).containsAny(CTString))
    assertFalse((T <:< CTNumber).containsAny(CTString))
  }

  @Test
  def shouldUnionTypeSpecs() {
    assertEquals(CTNumber | CTDouble | CTInteger | CTString,
      (T <:< CTNumber) | (T <:< CTString))
    assertEquals(CTNumber | CTDouble | CTInteger | CTCollection(CTString),
      (T <:< CTNumber) | (T <:< CTCollection(CTString)))
  }

  @Test
  def shouldIntersectTypeSpecs() {
    assertEquals(CTInteger: TypeSpec, T =:= CTInteger)
    assertEquals(CTInteger: TypeSpec, (T <:< CTNumber) =:= CTInteger)
    assertEquals(TypeSpec.none, (T <:< CTNumber) =:= CTString)

    assertEquals(CTNumber: TypeSpec, (CTNumber | CTInteger) & (CTAny | CTNumber))
    assertEquals(CTNumber: TypeSpec, (T >:> CTNumber) & (T <:< CTNumber))
    assertEquals(CTNumber: TypeSpec, (CTNumber | CTInteger) & (CTNumber | CTDouble))

    assertEquals(CTCollectionAny | CTCollection(CTCollectionAny),
      (T >:> CTCollection(CTCollectionAny)) & CTCollectionT)

    assertEquals(T <:< CTNumber,
      ((T <:< CTNumber) | CTCollectionT) & ((T <:< CTNumber) | (T <:< CTString)))
  }

  @Test
  def shouldConstrainTypeSpecs() {
    assertEquals(TypeSpec.exact(CTInteger), T <:< CTInteger)
    assertEquals(CTNumber | CTDouble | CTInteger, T <:< CTNumber)

    assertEquals(CTInteger: TypeSpec, CTInteger <:< CTNumber)
    assertEquals(CTInteger: TypeSpec, T <:< CTNumber <:< CTInteger)
    assertEquals(TypeSpec.none, CTNumber <:< CTInteger)

    assertEquals(CTInteger: TypeSpec,
      (CTInteger | CTString | CTMap) constrain (CTNode | CTNumber))
    assertEquals(TypeSpec.exact(CTCollection(CTString)),
      (CTInteger | CTCollection(CTString)) constrain CTCollectionAny)
    assertEquals(TypeSpec.none,
      (CTInteger | CTCollection(CTMap)) constrain CTCollection(CTNode))
    assertEquals(CTInteger | CTCollection(CTString),
      (CTInteger | CTCollection(CTString)) constrain CTAny)
  }

  @Test
  def constrainToCollectionTContainsEntireTree() {
    val constrainedToCollection = T <:< CTCollectionAny
    assertTrue(constrainedToCollection.contains(CTCollection(CTString)))
    assertTrue(constrainedToCollection.contains(CTCollection(CTInteger)))
    assertTrue(constrainedToCollection.contains(CTCollectionAny))
    assertTrue(constrainedToCollection.contains(CTCollection(CTCollection(CTInteger))))
    assertFalse(constrainedToCollection.contains(CTBoolean))
    assertFalse(constrainedToCollection.contains(CTAny))
  }

  @Test
  def constrainToBranchTypeWithinCollectionContainsAllSubtypes() {
    val constrainedToCollectionOfNumber = T <:< CTCollection(CTNumber)
    assertEquals(
      CTCollection(CTNumber) |
      CTCollection(CTDouble) |
      CTCollection(CTInteger)
    , constrainedToCollectionOfNumber)
  }

  @Test
  def constrainToSubTypeWithinCollection() {
    assertEquals(CTCollection(CTString): TypeSpec,
      CTCollectionT <:< CTCollection(CTString))
  }

  @Test
  def constrainToAnotherBranch() {
    assertEquals(TypeSpec.none,
      T <:< CTNumber <:< CTString)
  }

  @Test
  def unionTwoConstrainedBranches() {
    assertEquals(CTNumber | CTDouble | CTInteger | CTString,
      T <:< CTNumber | T <:< CTString)
  }

  @Test
  def constrainToSuperTypeOfOne() {
    val integerOrString = CTInteger | CTString
    assertEquals(CTInteger: TypeSpec, integerOrString <:< CTNumber)
  }

  @Test
  def constrainToAllTypes() {
    assertEquals(T, T constrain T)
    assertEquals(T <:< CTNumber, T <:< CTNumber constrain T)
  }

  @Test
  def constrainToDifferentBranch() {
    assertEquals(TypeSpec.none, (T <:< CTString) constrain (T <:< CTNumber))
  }

  @Test
  def constrainFromUnionedBranchesToSubTypeOfOneBranch() {
    val constrainedToNumberOrCollectionT = T <:< CTNumber | CTCollectionT
    val constrainedToCollectionOfNumber = T <:< CTCollection(CTNumber)
    assertEquals(constrainedToCollectionOfNumber, constrainedToNumberOrCollectionT constrain constrainedToCollectionOfNumber)
  }

  @Test
  def shouldMergeUpTypeSpecs() {
    assertEquals(CTNode | CTNumber | CTAny,
      (CTNode | CTNumber) mergeUp (CTNode | CTNumber))

    assertEquals(CTNode | CTNumber | CTAny,
      (CTNode | CTNumber) mergeUp (CTNode | CTNumber))
    assertEquals(CTNumber | CTAny,
      (CTNode | CTNumber) mergeUp CTNumber)
    assertEquals(CTNode | CTNumber | CTMap | CTAny,
      (CTNode | CTNumber) mergeUp (CTNode | CTNumber | CTRelationship))
    assertEquals(TypeSpec.exact(CTAny),
      (CTNode | CTNumber) >:> CTAny)
    assertEquals(TypeSpec.exact(CTAny),
      CTAny mergeUp (CTNode | CTNumber))

    assertEquals(TypeSpec.exact(CTMap),
      TypeSpec.exact(CTRelationship) mergeUp TypeSpec.exact(CTNode))
    assertEquals(CTMap | CTNumber | CTAny,
      (CTRelationship | CTInteger) mergeUp (CTNode | CTNumber))

    assertEquals(CTNumber | CTCollectionAny | CTAny,
      (CTInteger | CTCollection(CTString)) mergeUp (CTNumber | CTCollection(CTInteger)))
  }

  @Test
  def mergeUpToRootType() {
    val mergedWithAny = T >:> CTAny
    assertEquals(CTAny: TypeSpec, mergedWithAny)
  }

  @Test
  def mergeUpToLeafType() {
    val mergedWithInteger = T >:> CTInteger
    assertEquals(CTAny | CTNumber | CTInteger, mergedWithInteger)
  }

  @Test
  def mergeUpToCTCollection() {
    val mergedWithCollectionOfAny = T >:> CTCollectionAny
    assertEquals(CTAny | CTCollectionAny, mergedWithCollectionOfAny)

    val mergedWithCollectionOfString = T >:> CTCollection(CTString)
    assertEquals(CTAny | CTCollectionAny | CTCollection(CTString), mergedWithCollectionOfString)
  }

  @Test
  def mergeUpToMultipleTypes() {
    val mergedWithLongOrString = (T >:> CTInteger) | (T >:> CTString)
    assertEquals(CTAny | CTNumber | CTInteger | CTString, mergedWithLongOrString)

    val mergedWithCollectionOfStringOrLong = (T >:> CTCollection(CTString)) | (T >:> CTInteger)
    assertEquals(
      CTAny |
      CTNumber |
      CTInteger |
      CTCollectionAny |
      CTCollection(CTString)
    , mergedWithCollectionOfStringOrLong)
  }

  @Test
  def mergeUpFromDifferentBranchesToSingleSuperType() {
    val mergedWithCollectionOfStringOrLong = (T >:> CTCollection(CTString)) | (T >:> CTInteger)
    assertEquals(CTAny | CTNumber, mergedWithCollectionOfStringOrLong >:> CTNumber)
    assertEquals(CTAny | CTCollectionAny,
      mergedWithCollectionOfStringOrLong >:> CTCollectionAny)
  }

  @Test
  def mergeUpFromDifferentBranchesToSingleSubType() {
    val mergedWithCollectionOfStringOrNumber = (T >:> CTCollection(CTString)) | (T >:> CTNumber)
    assertEquals(CTAny | CTNumber, mergedWithCollectionOfStringOrNumber >:> CTInteger)
  }

  @Test
  def mergeUpFromConstrainedBranchToSubType() {
    assertEquals(CTNumber | CTInteger, (T <:< CTNumber) >:> CTInteger)
  }

  @Test
  def mergeUpFromConstrainedBranchToConstraintRoot() {
    assertEquals(CTNumber: TypeSpec, (T <:< CTNumber) >:> CTNumber)
  }

  @Test
  def mergeUpFromConstrainedBranchToSuperType() {
    val constrainedToLong = T <:< CTInteger
    assertEquals(CTNumber: TypeSpec, constrainedToLong >:> CTNumber)
  }

  @Test
  def mergeUpFromConstrainedBranchToTypeInDifferentBranch() {
    val constrainedToNumber = T <:< CTNumber
    assertEquals(CTAny: TypeSpec, constrainedToNumber >:> CTString)

    val constrainedToLong = T <:< CTInteger
    assertEquals(CTAny: TypeSpec, constrainedToLong >:> CTString)
  }

  @Test
  def mergeUpFromMultipleConstrainedBranchesToSubtypeOfSingleBranch() {
    val numberOrCollectionT = (T <:< CTNumber) | CTCollectionT
    assertEquals(CTAny | CTNumber | CTInteger, numberOrCollectionT >:> CTInteger)
    assertEquals(CTAny | CTCollectionAny | CTCollection(CTNumber) | CTCollection(CTInteger),
      numberOrCollectionT >:> CTCollection(CTInteger))
  }

  @Test
  def mergeUpWithIndefiniteSet() {
    assertEquals(CTAny: TypeSpec, CTAny mergeUp T)
    assertEquals(CTAny: TypeSpec, T mergeUp CTAny)
    assertEquals(CTAny: TypeSpec, CTAny mergeUp CTCollectionT)
    assertEquals(CTAny: TypeSpec, CTCollectionT mergeUp CTAny)
  }

  @Test
  def mergeUpFromBranchToSuperSet() {
    assertEquals(CTNumber | CTInteger,
      CTInteger mergeUp T <:< CTNumber)

    val mergedSet = T <:< CTCollection(CTCollectionAny) mergeUp T
    assertTrue(mergedSet.contains(CTCollection(CTCollection(CTString))))
    assertTrue(mergedSet.contains(CTCollection(CTCollection(CTInteger))))
    assertTrue(mergedSet.contains(CTCollection(CTCollection(CTNumber))))
    assertTrue(mergedSet.contains(CTCollection(CTCollectionAny)))
    assertFalse(mergedSet.contains(CTCollection(CTString)))
    assertFalse(mergedSet.contains(CTCollection(CTNumber)))
    assertTrue(mergedSet.contains(CTCollectionAny))
    assertFalse(mergedSet.contains(CTString))
    assertFalse(mergedSet.contains(CTNumber))
    assertTrue(mergedSet.contains(CTAny))
  }

  @Test
  def mergeUpFromBranchToSameSet() {
    assertEquals(T <:< CTNumber, T <:< CTNumber mergeUp (CTNumber | CTInteger | CTDouble))
  }

  @Test
  def mergeUpFromBranchToSubSet() {
    assertEquals(CTInteger | CTNumber,
      T <:< CTNumber mergeUp CTInteger)

    val fromCollectionOfCollectionT = T <:< CTCollection(CTCollectionAny)
    assertTrue((T mergeUp fromCollectionOfCollectionT).contains(CTCollection(CTCollection(CTString))))
    assertTrue((T mergeUp fromCollectionOfCollectionT).contains(CTCollection(CTCollection(CTInteger))))
    assertTrue((T mergeUp fromCollectionOfCollectionT).contains(CTCollection(CTCollection(CTNumber))))
    assertTrue((T mergeUp fromCollectionOfCollectionT).contains(CTCollection(CTCollectionAny)))
    assertFalse((T mergeUp fromCollectionOfCollectionT).contains(CTCollection(CTString)))
    assertFalse((T mergeUp fromCollectionOfCollectionT).contains(CTCollection(CTNumber)))
    assertTrue((T mergeUp fromCollectionOfCollectionT).contains(CTCollectionAny))
    assertFalse((T mergeUp fromCollectionOfCollectionT).contains(CTString))
    assertFalse((T mergeUp fromCollectionOfCollectionT).contains(CTNumber))
    assertTrue((T mergeUp fromCollectionOfCollectionT).contains(CTAny))
  }

  @Test
  def constrainFromMergedUpSpecToSuperType() {
    val allMergedWithLong = T >:> CTInteger
    assertEquals(CTNumber | CTInteger,
      allMergedWithLong <:< CTNumber)
  }

  @Test
  def constrainFromMergedUpSpecToMergeType() {
    val allMergedWithNumber = T >:> CTNumber
    assertEquals(CTNumber: TypeSpec,
      allMergedWithNumber <:< CTNumber)
  }

  @Test
  def constrainFromMergedUpSpecToSubType() {
    val allMergedWithNumber = T >:> CTNumber
    assertEquals(TypeSpec.none,
      allMergedWithNumber <:< CTInteger)
  }

  @Test
  def constrainFromMergedUpSpecToDifferentBranch() {
    val allMergedWithNumber = T >:> CTNumber
    assertEquals(TypeSpec.none,
      allMergedWithNumber <:< CTString)
  }

  @Test
  def constrainFromMergedUpSpecToSuperSet() {
    val allMergedWithLong = T >:> CTInteger
    val allMergedWithNumber = T >:> CTNumber
    assertEquals(CTAny | CTNumber,
      allMergedWithNumber constrain allMergedWithLong)
  }

  @Test
  def constrainFromMergedUpSpecToSameSet() {
    val allMergedWithNumber = T >:> CTNumber
    assertEquals(CTAny | CTNumber,
      allMergedWithNumber constrain allMergedWithNumber)
  }

  @Test
  def constrainFromMergedUpSpecToSubSet() {
    val allMergedWithLong = T >:> CTInteger
    val allMergedWithNumber = T >:> CTNumber
    assertEquals(CTAny | CTNumber | CTInteger,
      allMergedWithLong constrain allMergedWithNumber)
  }

  @Test
  def constrainFromMergedUpSpecToIntersectingSet() {
    val allMergedWithLong = T >:> CTInteger
    val allMergedWithString = T >:> CTString
    assertEquals(CTAny | CTNumber | CTInteger,
      allMergedWithLong constrain allMergedWithString)

    val allMergedWithStringAndNumber = (T >:> CTString) | (T >:> CTNumber)
    assertEquals(CTAny | CTNumber | CTInteger,
      allMergedWithLong constrain allMergedWithStringAndNumber)
  }

  @Test
  def mergeUpFromMergedUpSpecToSuperSet() {
    val allMergedWithLong = T >:> CTInteger
    val allMergedWithNumber = T >:> CTNumber
    assertEquals(CTAny | CTNumber,
      allMergedWithNumber mergeUp allMergedWithLong)
  }

  @Test
  def shouldReparentIntoCollection() {
    assertEquals(CTCollection(CTString) | CTCollection(CTCollection(CTNumber)),
      (CTString | CTCollection(CTNumber)).reparent(CTCollection))
    assertEquals(CTCollectionT,
      T.reparent(CTCollection))
  }

  @Test
  def shouldIdentifyCoercions() {
    assertEquals(CTBoolean: TypeSpec, (T <:< CTDouble).coercions)
    assertEquals(CTBoolean | CTDouble, (T <:< CTInteger).coercions)
    assertEquals(CTBoolean | CTDouble, (CTDouble | CTInteger).coercions)
    assertEquals(CTBoolean: TypeSpec, CTCollectionT.coercions)
    assertEquals(CTBoolean: TypeSpec, TypeSpec.exact(CTCollection(CTPath)).coercions)
    assertEquals(CTBoolean | CTDouble, TypeSpec.all.coercions)
    assertEquals(CTBoolean: TypeSpec, (T <:< CTCollectionAny).coercions)
  }

  @Test
  def shouldIntersectWithCoercions() {
    assertEquals(CTInteger: TypeSpec, T =%= CTInteger)
    assertEquals(CTDouble: TypeSpec, CTInteger =%= CTDouble)
    assertEquals(TypeSpec.none, CTNumber =%= CTDouble)
    assertEquals(CTBoolean: TypeSpec, CTCollectionT =%= CTBoolean)
    assertEquals(CTBoolean: TypeSpec, (T <:< CTNumber) =%= CTBoolean)
    assertEquals(CTBoolean: TypeSpec, CTCollectionT intersectWithCoercion (CTBoolean | CTString))
    assertEquals(TypeSpec.none, CTInteger =%= CTString)
  }

  @Test
  def shouldConstrainWithCoercions() {
    assertEquals(CTInteger: TypeSpec, T <%< CTInteger)
    assertEquals(CTDouble: TypeSpec, CTInteger <%< CTDouble)
    assertEquals(TypeSpec.none, CTNumber <%< CTDouble)
    assertEquals(CTBoolean: TypeSpec, CTCollectionT <%< CTBoolean)
    assertEquals(CTBoolean: TypeSpec, (T <:< CTNumber) <%< CTBoolean)
    assertEquals(CTBoolean: TypeSpec, CTCollectionT constrainWithCoercion (CTBoolean | CTString))
    assertEquals(TypeSpec.none, CTInteger <%< CTString)
  }

  @Test
  def equalTypeSpecsShouldEqual() {
    assertEquals(CTString: TypeSpec, CTString: TypeSpec)

    assertEquals(T <:< CTString, CTString: TypeSpec)
    assertEquals(CTString: TypeSpec, T <:< CTString)

    assertEquals(CTNumber | CTInteger | CTDouble, CTDouble | CTInteger | CTNumber)
    assertEquals(CTNumber | CTInteger | CTDouble, T <:< CTNumber)
    assertEquals(T <:< CTNumber, CTNumber | CTInteger | CTDouble)

    assertEquals(T <:< CTNumber, (T <:< CTNumber) | (T <:< CTInteger))
    assertNotEquals(T <:< CTNumber, (T <:< CTNumber) | (T <:< CTString))

    assertEquals(T, T)
    assertEquals(CTCollectionT, T <:< CTCollectionAny)
    assertEquals(T <:< CTCollectionAny, CTCollectionT)

    assertEquals(T, T | (T <:< CTString))
    assertEquals(T, T | T)
  }

  @Test
  def differentTypeSpecsShouldNotEqual() {
    assertNotEquals(T <:< CTNumber, T)
    assertNotEquals(T, T <:< CTNumber)

    assertNotEquals(T <:< CTNumber, CTCollectionT)
    assertNotEquals(CTCollectionT, T <:< CTNumber)

    assertNotEquals(T, CTNumber: TypeSpec)
    assertNotEquals(CTNumber: TypeSpec, T)
  }

  @Test
  def shouldHaveIndefiniteSizeWhenAllowingUnconstrainedAnyAtAnyDepth() {
    assertFalse(T.hasDefiniteSize)
    assertFalse(CTCollectionT.hasDefiniteSize)

    assertTrue((CTString | CTNumber).hasDefiniteSize)
    assertTrue((T <:< CTCollection(CTString)).hasDefiniteSize)

    assertFalse((T <:< CTCollection(CTCollectionAny)).hasDefiniteSize)
    assertTrue((T <:< CTCollection(CTCollection(CTString))).hasDefiniteSize)

    assertTrue((T >:> CTAny).hasDefiniteSize)
    assertTrue((CTCollectionT >:> CTCollectionAny).hasDefiniteSize)
  }

  @Test
  def shouldBeEmptyWhenNoPossibilitiesRemain() {
    assertFalse(T.isEmpty)
    assertTrue(TypeSpec.none.isEmpty)
    assertTrue(((T >:> CTNumber) intersect CTInteger).isEmpty)
  }

  @Test
  def shouldFormatNone() {
    assertEquals("()", TypeSpec.none.mkString("(", ", ", " or ", ")"))
  }

  @Test
  def shouldFormatSingleType() {
    assertEquals("(Any)", TypeSpec.exact(CTAny).mkString("(", ", ", " or ", ")"))
    assertEquals("<Node>", TypeSpec.exact(CTNode).mkString("<", ", ", " and ", ">"))
  }

  @Test
  def shouldFormatTwoTypes() {
    assertEquals("Any or Node", TypeSpec.exact(CTAny, CTNode).mkString("", ", ", " or ", ""))
    assertEquals("-Node or Relationship-", (CTRelationship | CTNode).mkString("-", ", ", " or ", "-"))
  }

  @Test
  def shouldFormatThreeTypes() {
    assertEquals("Integer, Node, Relationship", (CTRelationship | CTInteger | CTNode).mkString(", "))
    assertEquals("(Integer, Node, Relationship)", (CTRelationship | CTInteger | CTNode).mkString("(", ", ", ")"))
    assertEquals("(Any, Node or Relationship)", (CTRelationship | CTAny | CTNode).mkString("(", ", ", " or ", ")"))
    assertEquals("[Integer, Node and Relationship]", (CTRelationship | CTInteger | CTNode).mkString("[", ", ", " and ", "]"))
  }

  @Test
  def shouldFormatToStringForIndefiniteSizedSet() {
    assertEquals("T", T.mkString(", "))
    assertEquals("Collection<T>", CTCollectionT.mkString(", "))
    assertEquals("Boolean, Collection<T>", (CTCollectionT | CTBoolean).mkString(", "))
    assertEquals("Boolean, Collection<String>, Collection<Collection<T>>",
      ((T <:< CTCollection(CTCollectionAny)) | CTBoolean | CTCollection(CTString)).mkString(", "))
  }

  @Test
  def shouldFormatToStringForDefiniteSizedSet() {
    assertEquals("Any", TypeSpec.exact(CTAny).mkString(", "))
    assertEquals("String", TypeSpec.exact(CTString).mkString(", "))
    assertEquals("Double, Integer, Number", (T <:< CTNumber).mkString(", "))
    assertEquals("Boolean, Double, Integer, Number",
      ((T <:< CTNumber) | (T <:< CTBoolean)).mkString(", "))
    assertEquals("Any, Number", (T >:> CTNumber).mkString(", "))
    assertEquals("Boolean, String, Collection<Boolean>, Collection<String>",
      (T <:< CTBoolean | T <:< CTString | T <:< CTCollection(CTBoolean) | T <:< CTCollection(CTString)).mkString(", "))
    assertEquals("Boolean, String, Collection<Boolean>, Collection<Collection<String>>",
      (T <:< CTBoolean | T <:< CTString | T <:< CTCollection(CTBoolean) | T <:< CTCollection(CTCollection(CTString))).mkString(", "))
    assertEquals("Any, Collection<Any>", (T >:> CTCollectionAny).mkString(", "))
  }

  @Test
  def shouldIterateOverDefiniteSizedSet() {
    assertEquals(Seq(CTString),
      TypeSpec.exact(CTString).iterator.toSeq)
    assertEquals(Seq(CTDouble, CTInteger, CTNumber),
      (T <:< CTNumber).iterator.toSeq)
    assertEquals(Seq(CTBoolean, CTDouble, CTInteger, CTNumber),
      ((T <:< CTNumber) | (T <:< CTBoolean)).iterator.toSeq)
    assertEquals(Seq(CTAny, CTNumber),
      (T >:> CTNumber).iterator.toSeq)
    assertEquals(Seq(CTBoolean, CTString, CTCollection(CTBoolean), CTCollection(CTString)),
      (CTBoolean | CTString | CTCollection(CTBoolean) | CTCollection(CTString)).iterator.toSeq)
    assertEquals(Seq(CTBoolean, CTString, CTCollection(CTBoolean), CTCollection(CTCollection(CTString))),
      (CTBoolean | CTString | CTCollection(CTBoolean) | CTCollection(CTCollection(CTString))).iterator.toSeq)
  }
}
