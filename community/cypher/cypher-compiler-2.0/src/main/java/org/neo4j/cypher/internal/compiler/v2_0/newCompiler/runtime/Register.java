package org.neo4j.cypher.internal.compiler.v2_0.newCompiler.runtime;

public interface Register {
    void setObject(int idx, Object value);
    void setLong(int idx, long value);

    Object getObject(int idx);
    long getLong(int idx);

    Register copy();
}
