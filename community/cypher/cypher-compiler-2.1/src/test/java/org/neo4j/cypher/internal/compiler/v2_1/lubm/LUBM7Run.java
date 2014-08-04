package org.neo4j.cypher.internal.compiler.v2_1.lubm;

/**
 * Created by andrey on 27/07/14.
 */
public class LUBM7Run extends LUBMRun {
    @Override
    public void run(int repeats) {
        String query = "cypher 2.1.experimental MATCH (x)-[:TakesCourse]->(y)<-[:TeacherOf]-(z:Professor)" +
                "where z.id = '<http://www.Department0.University0.edu/AssociateProfessor0>' return y,z";
        compileAndRun(query, repeats);

    }
}
