package org.neo4j.cypher

import org.junit.Test
import org.scalacheck.Prop._
import org.scalacheck._
import org.scalacheck.Gen._
import org.neo4j.graphdb.{Node, DynamicLabel}
import java.util.concurrent.TimeUnit
import collection.JavaConverters._
import org.scalacheck.Test.{Failed, Parameters}
import org.neo4j.cypher.internal.pipes.QueryStateHelper
import org.neo4j.cypher.internal.commands.expressions.StringHelper
import scala.collection.mutable

class ScalaCheckRegexp extends ExecutionEngineHelper with Neo4jPropertyGenerator with StringHelper {

  val label = DynamicLabel.label("FOO")

  @Test def apa() {
    graph.inTx {
      graph.schema().indexFor(label).on("indexed").create()
    }
    graph.inTx {
      graph.schema().awaitIndexesOnline(10, TimeUnit.SECONDS)
    }

    val params = new Parameters.Default {
      override val minSuccessfulTests: Int = 10000
    }

    val seen = new mutable.HashMap[Node, Any]()

    val prop = forAll(anyPropertyValue) {
      x =>
        graph.inTx {
          val node = graph.createNode(label)
          node.setProperty("indexed", x)
          node.setProperty("unindexed", x)
          seen += node -> x
        }
        graph.inTx {
          val unindexedHits = graph.findNodesByLabelAndProperty(label, "unindexed", x).asScala.toSet
          val indexedHits = graph.findNodesByLabelAndProperty(label, "indexed", x).asScala.toSet
          val b = unindexedHits == indexedHits
          if (!b) {
            val state = QueryStateHelper.queryStateFrom(graph)

            if (indexedHits.size > unindexedHits.size) {
              val missing = indexedHits -- unindexedHits

              val otherVal = seen(missing.head)
              println(otherVal)
              println("These nodes where found through the index")

              println(missing.map(x => text(x, state.inner)))
            } else {
              val missing = unindexedHits -- indexedHits
              println("These nodes where not found through the index")
              println(missing.map(x => text(x, state.inner)))
            }
          }
          b
        }
    }

    val result = org.scalacheck.Test.check(params, prop)
    result.status match {
      case Failed(args, lbl) =>
        val output = args.head.arg match {
          case arr: Array[_] => arr.toSeq.mkString("[", ", ", "]")
          case x             => x
        }

        println("Failed with " + output)

      case _ => println("OK!")
    }
  }
}

trait Neo4jPropertyGenerator {
  def strings: Gen[String] = alphaStr

  def integers: Gen[Int] = chooseNum(Int.MinValue, Int.MaxValue)

  def longs: Gen[Long] = chooseNum(Long.MinValue, Long.MaxValue)

  def bools: Gen[Boolean] = Gen.oneOf(true, false)

  def chars: Gen[Char] = Gen.alphaChar

  def floats: Gen[Float] = chooseNum(Float.MinValue, Float.MaxValue)

  def doubles: Gen[Double] = chooseNum(Double.MinValue, Double.MaxValue)

  def bytes: Gen[Byte] = chooseNum(Byte.MinValue, Byte.MaxValue)

  def shorts: Gen[Short] = chooseNum(Short.MinValue, Short.MaxValue)

  private def arrayOf[T: Manifest](in: Gen[T]): Gen[Array[T]] = listOf(in).map(_.toArray)

  def scalar: Gen[Any] = strings | integers | longs | bools | chars | floats | doubles | bytes | shorts

  def arrays: Gen[Array[_]] = arrayOf(strings) | arrayOf(integers) | arrayOf(longs) | arrayOf(bools) | arrayOf(chars) |
    arrayOf(floats) | arrayOf(doubles) | arrayOf(bytes) | arrayOf(shorts)

  def anyPropertyValue = scalar | arrays
}