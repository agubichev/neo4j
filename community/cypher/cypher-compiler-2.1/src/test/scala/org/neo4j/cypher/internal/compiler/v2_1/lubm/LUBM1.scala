package org.neo4j.cypher.internal.compiler.v2_1.lubm

/**
 * Created by andrey on 26/07/14.
 */
class LUBM1 extends LUBMHelper{
  test("LUBM Query 1") {
    val query = "cypher 2.1.experimental MATCH (x:GraduateStudent)-[p1:TakesCourse]->(z:Course)" +
      " where z.id='<http://www.Department0.University0.edu/GraduateCourse0>' return x"
    compileAndRun(query)

  }

}
