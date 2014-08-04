package org.neo4j.cypher.internal.compiler.v2_1.lubm

/**
 * Created by andrey on 26/07/14.
 */
class LUBM11 extends LUBMHelper{
  test("LUBM Query 10") {
    val query = "cypher 2.1.experimental MATCH (x)-[:SubOrganizationOf*1..2]-(y:University)" +
      "where y.id='<http://www.University0.edu>' return x"
    compileAndRun(query)

  }

}
