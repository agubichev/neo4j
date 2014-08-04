package org.neo4j.cypher.internal.compiler.v2_1.lubm;

/**
 * Created by andrey on 27/07/14.
 */
public class LUBM12Run extends LUBMRun {
    @Override
    public void run(int repeats) {
        String query = "cypher 2.1.experimental match (x:FullProfessor)-[:WorksFor]->(y:Department), " +
                "(y:Department)-[:SubOrganizationOf*1..2]->(uni:University),(x:FullProfessor)-[:HeadOf]->(z:Department)" +
                "where (uni.id='<http://www.University0.edu>') return x, y";
        compileAndRun(query,repeats);

    }
}
