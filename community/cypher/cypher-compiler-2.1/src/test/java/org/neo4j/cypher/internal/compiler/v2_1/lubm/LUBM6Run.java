package org.neo4j.cypher.internal.compiler.v2_1.lubm;

/**
 * Created by andrey on 27/07/14.
 */
public class LUBM6Run extends LUBMRun {
    @Override
    public void run(int repeats) {
        String query = "cypher 2.1.experimental MATCH (x:Student) return count(x)";
        compileAndRun(query, repeats);

    }
}
