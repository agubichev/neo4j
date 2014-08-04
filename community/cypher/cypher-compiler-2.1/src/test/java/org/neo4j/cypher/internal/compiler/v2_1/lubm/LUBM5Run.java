package org.neo4j.cypher.internal.compiler.v2_1.lubm;

/**
 * Created by andrey on 27/07/14.
 */
public class LUBM5Run extends LUBMRun {
    @Override
    public void run(int repeats) {
        String query = "cypher 2.1.experimental MATCH (x)-[p1:MemberOf]->(y:Department)" +
                " where y.id = '<http://www.Department0.University0.edu>' return x";
        compileAndRun(query, repeats);

    }
}
