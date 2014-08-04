package org.neo4j.cypher.internal.compiler.v2_1.lubm;

/**
 * Created by andrey on 27/07/14.
 */
public class LUBM14Run extends LUBMRun {
    @Override
    public void run(int repeats) {
        String query = "cypher 2.1.experimental match (x:UndergraduateStudent) return count(x)";
        compileAndRun(query,repeats);
    }
}
