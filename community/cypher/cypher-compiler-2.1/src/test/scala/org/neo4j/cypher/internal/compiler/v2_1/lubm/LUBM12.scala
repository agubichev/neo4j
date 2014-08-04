package org.neo4j.cypher.internal.compiler.v2_1.lubm

/**
 * Andrey Gubichev, 18/07/14.
 */
class LUBM12 extends LUBMHelper{
  test("LUBM Query 12") {
    val query = "cypher 2.1.experimental match (x:FullProfessor)-[:WorksFor]->(y:Department), " +
      "(y:Department)-[:SubOrganizationOf*1..2]->(uni:University),(x:FullProfessor)-[:HeadOf]->(z:Department)" +
      "where (uni.id='<http://www.University0.edu>') return x, y"
     compileAndRun(query)

    //var times = List.fill(50)(runExperiment(handWrittenPlan,"Expands"))
    //println(times)
    //println("median: "+median(times))


  }

}
