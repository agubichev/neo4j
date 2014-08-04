package org.neo4j.cypher.internal.compiler.v2_1.lubm

/**
 * Andrey Gubichev, 17/07/14.
 */
class LUBM5 extends LUBMHelper{
  test("LUBM Query 5") {
    val query = "cypher 2.1.experimental MATCH (x)-[p1:MemberOf]->(y:Department)" +
      " where y.id = '<http://www.Department0.University0.edu>' return x"
    compileAndRun(query)
  }
}
