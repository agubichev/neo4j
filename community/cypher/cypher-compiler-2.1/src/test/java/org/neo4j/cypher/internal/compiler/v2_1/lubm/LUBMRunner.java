package org.neo4j.cypher.internal.compiler.v2_1.lubm;

import org.neo4j.cypher.ExecutionEngine;
import org.neo4j.cypher.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.StringLogger;

import java.util.LinkedList;

/**
 * Created by andrey on 21/07/14.
 */
public class LUBMRunner {

    public static void main(String[] args){
        System.out.println("LUBM runner");
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(System.getProperty("LUBMpath")/*"Users/andrey/Desktop/lubm50inf"*/);
        ExecutionEngine engine = new ExecutionEngine(db, StringLogger.DEV_NULL);

       /* LinkedList<LUBMRun> queries = new LinkedList<LUBMRun>();
        queries.add(new LUBM2Run());
        queries.add(new LUBM4Run());
        queries.add(new LUBM5Run());
        queries.add(new LUBM6Run());
        queries.add(new LUBM7Run());
        queries.add(new LUBM8Run());
        queries.add(new LUBM9Run());
        queries.add(new LUBM12Run());
        queries.add(new LUBM14Run());

        for (int i = 0; i < queries.size(); i++){
            queries.get(i).run(100);
        }
*/
        for (int i = 0; i < 10; i++) {
            double start = System.currentTimeMillis();
            ExecutionResult res = engine.execute("cypher 2.1.experimental match (x:UndergraduateStudent) return x");
            res.dumpToString();
            double stop = System.currentTimeMillis();
            System.out.println("TIME: "+(stop-start)+ " ms");
        }

    }
}
