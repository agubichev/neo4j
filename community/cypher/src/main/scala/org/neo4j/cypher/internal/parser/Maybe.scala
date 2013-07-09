package org.neo4j.cypher.internal.parser


abstract sealed class Maybe[+T] {
  def values: Seq[T]

  def success: Boolean

  def ++[B >: T](other: Maybe[B]): Maybe[B]

  def map[B](f: T => B): Maybe[B]

  def seqMap[B](f: Seq[T] => Seq[B]): Maybe[B]

  def getValuesOr(f: => Seq[T]): Seq[T]
}

case class Yes[T](values: Seq[T]) extends Maybe[T] {
  def success = true

  def ++[B >: T](other: Maybe[B]): Maybe[B] = other match {
    case Yes(otherStuff) => Yes(values ++ otherStuff)
    case No(msg) => No(msg)
  }

  def map[B](f: T => B): Maybe[B] = Yes(values.map(f))

  def seqMap[B](f: (Seq[T]) => Seq[B]): Maybe[B] = Yes(f(values))

  def getValuesOr(f: => Seq[T]): Seq[T] = values
}

case class No(messages: Seq[String]) extends Maybe[Nothing] {
  def values = throw new Exception("No values exists")

  def success = false

  def ++[B >: Nothing](other: Maybe[B]): Maybe[B] = other match {
    case Yes(_) => this
    case No(otherMessages) => No(messages ++ otherMessages)
  }

  def map[B](f: Nothing => B): Maybe[B] = this

  def seqMap[B](f: (Seq[Nothing]) => Seq[B]): Maybe[B] = this

  def getValuesOr(f: => Seq[Nothing]): Seq[Nothing] = f
}
