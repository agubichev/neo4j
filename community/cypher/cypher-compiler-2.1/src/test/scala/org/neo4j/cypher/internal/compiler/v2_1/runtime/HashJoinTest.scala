package org.neo4j.cypher.internal.compiler.v2_1.runtime

import org.scalatest.Assertions
import org.neo4j.cypher.internal.compiler.v2_1.runtime.{StatementContext, Runtime}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.kernel.impl.util.FileUtils
import java.io.File
import org.neo4j.graphdb.{Transaction, GraphDatabaseService}
import org.junit.Test
import org.scalatest.mock.MockitoSugar
import org.neo4j.cypher.internal.compiler.v2_1.{ast, parser}
import org.parboiled.scala._
import org.neo4j.cypher.internal.compiler.v2_1.planner.{AbstractPlan, PlanGenerator, Id, QueryGraph}
import scala.Some
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.api.Statement
import scala.collection.JavaConversions._
import java.util
import scala.util.Random

class HashJoinTest extends Assertions with MockitoSugar {

  case class TestIdScan(data: Seq[java.lang.Long], value: Register[java.lang.Long]) extends Operator {
    var offset = 0
    def open() = offset = 0

    def next(): Boolean = {
      if (offset >= data.size)
        return false
      else {
        value.value = data(offset)
        offset+=1
        return true
      }
    }
    def close() = offset = 0
  }

  case class TestTupleScan(data: Seq[Seq[java.lang.Long]], val values: Seq[Register[java.lang.Long]]) extends Operator {
    var offset = 0
    def open() = {
      offset = 0
      if (data.size > 0 && data(0).size != values.size) {
        throw new Exception("Size of input tuples is not the same as the number of registers")
      }
    }

    def next(): Boolean = {
      if (offset >= data.size)
        return false
      else {
        (0 to data.size).zip(values).map{case (index, reg) => reg.value = data(offset)(index)}
        offset += 1
        return true
      }
    }

    def close() = {}
  }

  case class PrintOperator(input: Operator, value: Register[java.lang.Long]) extends Operator {
    def open() = input.open()

    def next(): Boolean = {
      val continue = input.next()
      if (continue) {
        println(value.value)
      }
      return continue
    }
    def close() = input.close()
  }

  case class PrintTupleOperator(input: Operator, values: Seq[Register[java.lang.Long]]) extends Operator {
    def open() = input.open()

    def next(): Boolean = {
      val continue = input.next()
      if (continue) {
        values.map{x => print(x.value + " ")}
        println()
      }
      return continue
    }
    def close() =  input.close()
  }

  case class ResultProducer(input: Operator, value: Register[java.lang.Long], var result: Seq[java.lang.Long])  extends Operator {
    def open() = input.open()

    def next(): Boolean = {
      val continue = input.next()
      if (continue){
        result = result :+ value.value
      }
      return continue
    }

    def close() = {
      input.close()
    }
  }


  @Test def hashJoinSimpleTest() {

    val value1: Register[java.lang.Long] = new Register[java.lang.Long]
    val op1: Operator = new TestIdScan(Seq(1,3,4,5), value1)
    val op2: Operator = new TestIdScan(Seq(3,4,5,6), value1)

    val leftTail = new java.util.ArrayList[Register[java.lang.Long]]()
    val leftObjTail = new java.util.ArrayList[Register[java.lang.Object]]()

    val join = new HashJoinOp(value1, leftTail, leftObjTail, op1, op2)

    val printer = new ResultProducer(join, value1, Seq.empty)

    printer.open()
    while (printer.next()) {
    }
    printer.close()

    assert(printer.result == Seq(3,4,5))
  }

  @Test def tailValuesTest() {
    var values1: Seq[Register[java.lang.Long]] = Seq.empty
    var values2: Seq[Register[java.lang.Long]] = Seq.empty

    (0 to 1) foreach { _ =>
      values1 = values1.+:(new Register[java.lang.Long])
      values2 = values2.+:(new Register[java.lang.Long])
    }

    val joinKey = new Register[java.lang.Long]
    println(values1.size)

    val op1: Operator = new TestTupleScan(Seq(Seq(1,2,3), Seq(1,3,4), Seq(2,3,4), Seq(2,4,6), Seq(3,6,3)), values1.+:(joinKey) )
    val op2: Operator = new TestTupleScan(Seq(Seq(1,10,11), Seq(1,23,34), Seq(2,33,44), Seq(6,40,60)), values2.+:(joinKey))

    /// Ugly conversion from Seq to java ArrayList
    val leftIdArray = new java.util.ArrayList[Register[java.lang.Long]](seqAsJavaList(values1))
    val leftObjArray = new util.ArrayList[Register[java.lang.Object]](seqAsJavaList(Seq.empty))

    val join: Operator = new HashJoinOpSimple(joinKey, leftIdArray, leftObjArray, op1, op2)

    //val printer: Operator = new PrintTupleOperator(join, (values1.+:(joinKey)) ++ values2)

    join.open()

    while (join.next()) {}

    join.close()

  }

  @Test def microBenchmark() {
    val value1: Register[java.lang.Long] = new Register[java.lang.Long]
    val n = 10000
    val factor = 1023
    val data1: Seq[java.lang.Long] = Seq.fill(n)(new java.lang.Long(math.abs(Random.nextInt) % factor))
    val data2: Seq[java.lang.Long] = Seq.fill(3*n)(new java.lang.Long(math.abs(Random.nextInt) % factor))
    val op1: Operator = new TestIdScan(data1, value1)
    val op2: Operator = new TestIdScan(data2, value1)

    val leftTail = new java.util.ArrayList[Register[java.lang.Long]]()
    val leftObjTail = new java.util.ArrayList[Register[java.lang.Object]]()

    val join_old = new HashJoinOp(value1, leftTail, leftObjTail, op1, op2)

    var start = System.nanoTime()
    join_old.open()
    while (join_old.next()) {}
    join_old.close()
    var end = System.nanoTime()

    System.out.println("Old join: " +(end-start)/1000000 + " ms");

    val join_new = new HashJoinOpSimple(value1, leftTail, leftObjTail, op1, op2)

    start = System.nanoTime()
    join_new.open()
    while (join_new.next()) {}
    join_new.close()
    end = System.nanoTime()
    System.out.println("New join: " +(end-start)/1000000 + " ms");

  }
}
