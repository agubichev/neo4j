package org.neo4j.cypher.internal.compiler.v2_1.lubm

/**
 * Created by andrey on 26/07/14.
 */
class LUBM10 extends LUBMHelper{
  test("LUBM Query 10") {
    val query = "cypher 2.1.experimental MATCH (x:Student)-[:TakesCourse]->(y:Course)" +
      "where y.id='<http://www.Department0.University0.edu/GraduateCourse0>' return x"
    compileAndRun(query)

  }

}
