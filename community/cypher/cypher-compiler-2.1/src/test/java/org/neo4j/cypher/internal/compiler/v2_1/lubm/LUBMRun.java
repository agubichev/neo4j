package org.neo4j.cypher.internal.compiler.v2_1.lubm;

import org.neo4j.cypher.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.util.StringLogger;
import scala.collection.immutable.List;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

/**
 * Created by andrey on 27/07/14.
 */
public abstract class LUBMRun {

    public abstract void run(int repeats);

    public double median(ArrayList<Double> input){
        Collections.sort(input);
        return input.get(input.size()/2);
    }


    public void compileAndRun(String query, int repeats){
        double t0 = 0;
        double t1 = 0;
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(System.getProperty("LUBMpath")/*"/Users/andrey/Desktop/data/lubm50inf"*/);
        ExecutionEngine engine = new ExecutionEngine(db, StringLogger.DEV_NULL);

        ArrayList<Double> times = new ArrayList<Double>();

        for (int i = 0; i < repeats; i++){
            t0 = System.currentTimeMillis();
            List res = engine.execute(query).toList();
            t1 = System.currentTimeMillis();
            times.add(t1-t0);

        }

        System.out.print("[");
        for (int i = 0; i < times.size()-1; i++)
            System.out.print(times.get(i)+", ");
        System.out.println(times.get(times.size() - 1) + "]");


        System.out.println("median: "+median(times));
        db.shutdown();
    }

}
