package org.neo4j.cypher.internal.compilator.v2_0;

public interface ResultVisitor {
    void accept(Result result);
}
