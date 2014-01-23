package org.neo4j.cypher.internal.compiler.v2_0.newCompiler.runtime;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class HashJoinOp implements Operator {
    private final int joinKeyId;
    private final int[] lhsTailLongIdx;
    private final int[] lhsTailObjectIdx;
    private final Operator lhs;
    private final Operator rhs;
    private final Register register;
    private final Map<Long, List<Register>> bucket = new HashMap<>();
    private int bucketPos = 0;
    private List<Register> currentBucketEntry = null;

    public HashJoinOp(int joinKeyId, int[] lhsTailLongIdx, int[] lhsTailObjectIdx, Operator lhs, Operator rhs, Register register) {
        this.joinKeyId = joinKeyId;
        this.lhsTailLongIdx = lhsTailLongIdx;
        this.lhsTailObjectIdx = lhsTailObjectIdx;
        this.lhs = lhs;
        this.rhs = rhs;
        this.register = register;

        fillHashBucket();
    }

    @Override
    public void open() {
        lhs.open();
        rhs.open();
    }

    @Override
    public boolean next() {
        while (currentBucketEntry == null || bucketPos >= currentBucketEntry.size()) {
            // If we've emptied our rhs, we're done here
            if (!rhs.next()) {
                return false;
            }

            // let's see if we find a match
            produceMatchIfPossible();
        }

        // We've found a match! Let's copy the data over.
        restoreFromTailEntry();

        return true;
    }

    private void produceMatchIfPossible() {
        long key = register.getLong(joinKeyId);
        currentBucketEntry = bucket.get(key);
        bucketPos = 0;
    }

    @Override
    public void close() {
        rhs.close();
        lhs.close();
    }

    private void fillHashBucket() {
        while (lhs.next()) {
            long key = register.getLong(joinKeyId);
            List<Register> objects = getTailEntriesForId(key);

            Register tailEntry = copyToTailEntry();

            objects.add(tailEntry);
        }
    }

    private List<Register> getTailEntriesForId(long key) {
        List<Register> objects = bucket.get(key);
        if (objects == null) {
            objects = new LinkedList<>();
            bucket.put(key, objects);
        }
        return objects;
    }

    private void restoreFromTailEntry() {
        int idx = bucketPos++;
        Register from = currentBucketEntry.get(idx);
        Register to = register;

        for (int i = 0; i < lhsTailLongIdx.length; i++) {
            long temp = from.getLong(i);
            to.setLong(lhsTailLongIdx[i], temp);
        }

        for (int i = 0; i < lhsTailObjectIdx.length; i++) {
            Object temp = from.getObject(i);
            to.setObject(lhsTailObjectIdx[i], temp);
        }
    }

    private Register copyToTailEntry() {
        Register tailEntry = new MapRegister();

        for (int i = 0; i < lhsTailLongIdx.length; i++) {
            long temp = register.getLong(lhsTailLongIdx[i]);
            tailEntry.setLong(i, temp);
        }

        for (int i = 0; i < lhsTailObjectIdx.length; i++) {
            Object temp = register.getObject(lhsTailLongIdx[i]);
            tailEntry.setObject(i, temp);
        }

        return tailEntry;
    }
}
