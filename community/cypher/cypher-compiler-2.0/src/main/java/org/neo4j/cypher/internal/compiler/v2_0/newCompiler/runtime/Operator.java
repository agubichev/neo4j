package org.neo4j.cypher.internal.compiler.v2_0.newCompiler.runtime;

public interface Operator {
    void open();

    boolean next();

    void close();
}
