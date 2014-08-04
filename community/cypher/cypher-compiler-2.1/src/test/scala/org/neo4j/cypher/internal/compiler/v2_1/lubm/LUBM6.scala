package org.neo4j.cypher.internal.compiler.v2_1.lubm

/**
 * Andrey Gubichev, 17/07/14.
 */
class LUBM6 extends LUBMHelper{
  test("LUBM Query 6") {
    val query = "cypher 2.1.experimental MATCH (x:Student) return count(x)"
    compileAndRun(query)
  }

}
