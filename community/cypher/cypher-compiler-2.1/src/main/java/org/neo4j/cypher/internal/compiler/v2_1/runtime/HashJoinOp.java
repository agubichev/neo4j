/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v2_1.runtime;

import java.util.*;

public class HashJoinOp implements Operator {
    private final Register<Long> joinRegister;
    private final ArrayList<Register<Long>> lhsIdTail;
    private final ArrayList<Register<Object>> lhsObjectTail;
    private final Operator lhs;
    private final Operator rhs;

    // Hash table entry: just a wrapper around the tail IDs and Objects
    private class Entry {
        public Long[] idTailValues;
        public Object[] objTailValues;

        public Entry(int idCount, int objCount){
            idTailValues = new Long[idCount];
            objTailValues = new Object[objCount];
        }
    }

    private final Map<Long, List<Entry>> bucket = new HashMap<>();
    private int bucketPos = 0;
    private List<Entry> currentBucketEntry = null;

    public HashJoinOp(Register<Long> joinRegister, ArrayList<Register<Long>> lhsIdTail, ArrayList<Register<Object>> lhsObjectTail,
                      Operator lhs, Operator rhs) {
        this.joinRegister = joinRegister;
        this.lhsIdTail = lhsIdTail;
        this.lhsObjectTail = lhsObjectTail;
        this.lhs = lhs;
        this.rhs = rhs;
    }

    @Override
    public void open() {
        lhs.open();
        rhs.open();

        fillHashBucket();
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
        long key = joinRegister.value;
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
            Long key = joinRegister.value;
            List<Entry> entries = getTailEntriesForId(key);
            Entry tailEntry = copyToTailEntry();
            entries.add(tailEntry);
        }
    }

    private List<Entry> getTailEntriesForId(long key) {
        List<Entry> objects = bucket.get(key);
        if (objects == null) {
            objects = new LinkedList<>();
            bucket.put(key, objects);
        }
        return objects;
    }

    private void restoreFromTailEntry() {
        int idx = bucketPos++;
        Entry from = currentBucketEntry.get(idx);
        //Register to = registers;

        for (int i = 0; i < lhsIdTail.size(); i++) {
            lhsIdTail.get(i).value = from.idTailValues[i];
        }

        for (int i = 0; i < lhsObjectTail.size(); i++) {
            lhsObjectTail.get(i).value = from.objTailValues[i];
        }
    }

    private Entry copyToTailEntry() {
        Entry tailEntry = new Entry(lhsIdTail.size(), lhsObjectTail.size());

        for (int i = 0; i < lhsIdTail.size(); i++) {
            tailEntry.idTailValues[i] = lhsIdTail.get(i).value;
        }

        for (int i = 0; i < lhsObjectTail.size(); i++) {
            tailEntry.objTailValues[i] = lhsObjectTail.get(i).value;
        }

        return tailEntry;
    }
}
