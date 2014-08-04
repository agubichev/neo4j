package org.neo4j.cypher.internal.compiler.v2_1.lubm;

import org.neo4j.cypher.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Created by andrey on 27/07/14.
 */
public class LUBM2Run extends LUBMRun {

    @Override
    public void run(int repeats) {
        String query = "cypher 2.1.experimental MATCH (x:GraduateStudent)-[p1:MemberOf]->(z:Department)-[p3:SubOrganizationOf]->(y:University)" +
                ", (x:GraduateStudent)-[p2:UndergraduateDegreeFrom]->(y:University) return x,y,z";

        compileAndRun(query, repeats);
    }
}
