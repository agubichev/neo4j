package org.neo4j.cypher.internal.compiler.v2_1.lubm

/**
 * Andrey Gubichev, 18/07/14.
 */
class LUBM14 extends LUBMHelper{
  test("LUBM Query 14") {
    val query = "cypher 2.1.experimental match (x:UndergraduateStudent) return count(x)"
    compileAndRun(query)

    //var times = List.fill(50)(runExperiment(handWrittenPlan,"Expands"))
    //println(times)
    //println("median: "+median(times))


  }


}
