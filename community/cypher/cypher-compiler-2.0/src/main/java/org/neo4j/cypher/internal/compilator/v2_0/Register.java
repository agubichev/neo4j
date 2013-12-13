package org.neo4j.cypher.internal.compilator.v2_0;

public class Register {
    private long[] nodes;
    private Object[] objects;

    public Register(int nodeCount, int objectCount) {
        nodes = new long[nodeCount];
        objects = new Object[objectCount];
    }

    public void setNode(int slotIndex, long nodeId) {
        nodes[slotIndex] = nodeId;
    }

    public long getNode(int slotIndex) {
        return nodes[slotIndex];
    }

    public void setObject(int slotIndex, Object object) {
        objects[slotIndex] = object;
    }

    public Object getObject(int slotIndex) {
        return objects[slotIndex];
    }
}
