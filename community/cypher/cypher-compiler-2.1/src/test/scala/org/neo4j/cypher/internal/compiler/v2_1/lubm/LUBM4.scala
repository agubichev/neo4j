package org.neo4j.cypher.internal.compiler.v2_1.lubm

/**
 * Andrey Gubichev, 17/07/14.
 */
class LUBM4 extends LUBMHelper {
  test("LUBM Query 4") {
    val query = "cypher 2.1.experimental MATCH (x:Professor)-[p1:WorksFor]->(y:Department)" +
      " where y.id = '<http://www.Department0.University0.edu>' return x.name, x.emailAddress, x.telephone"
    compileAndRun(query)
    /*var t0: Double = 0
    var t1: Double = 0
    var times: List[Double] = List.fill(100){
      t0 = System.nanoTime: Double
      val res = engine.execute(query).toList
      t1 = System.nanoTime: Double
      //times = (t1-t0)/ 1000000.0 :: times
      println("Compiled plan: Elapsed time " + (t1 - t0) / 1000000.0 + " msecs")
      println("Compiled plan: Result size " + res.size)
      (t1-t0)/1000000.0
    }
    println(times)
    println("median: "+median(times))*/

  }
}
