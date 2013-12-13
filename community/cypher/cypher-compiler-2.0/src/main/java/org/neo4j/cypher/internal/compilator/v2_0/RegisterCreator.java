package org.neo4j.cypher.internal.compilator.v2_0;

public class RegisterCreator {

    private final int nodeCount;
    private final int objectCount;

    public RegisterCreator(int nodeCount, int objectCount) {
        this.nodeCount = nodeCount;
        this.objectCount = objectCount;
    }

    public Register create() {
        return new Register(nodeCount, objectCount);
    }
}
