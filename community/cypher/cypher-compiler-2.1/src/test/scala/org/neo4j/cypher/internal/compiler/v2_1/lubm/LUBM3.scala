package org.neo4j.cypher.internal.compiler.v2_1.lubm

/**
 * Created by andrey on 26/07/14.
 */
class LUBM3 extends LUBMHelper{
  test("LUBM Query 3") {
    val query = "cypher 2.1.experimental MATCH (x:Publication)-[:PublicationAuthor]->(y:Professor)" +
      "where y.id='<http://www.Department0.University0.edu/AssistantProfessor0>' return x,y"
    compileAndRun(query)

  }

}
