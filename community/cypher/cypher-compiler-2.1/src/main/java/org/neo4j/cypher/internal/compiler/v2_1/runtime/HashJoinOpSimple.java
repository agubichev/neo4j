package org.neo4j.cypher.internal.compiler.v2_1.runtime;

import java.util.*;

public class HashJoinOpSimple implements Operator{
    private class Entry {
        public Entry next;
        public int key;
        public long[] idValues;
        public Object[] objectValues;

        public Entry(int key, int idValuesCount, int objectValuesCount) {
            next = null;
            this.key = key;
            this.idValues = new long[idValuesCount];
            this.objectValues = new Object[objectValuesCount];
        }
    }

    private final Register<Long> joinRegister;
    private final ArrayList<Register<Long>> lhsIdTail;
    private final ArrayList<Register<Object>> lhsObjectTail;
    private final Operator lhs;
    private final Operator rhs;
    private ArrayList<Entry> hashTable;
    private Entry hashTableIter;

    public HashJoinOpSimple(Register<Long> joinRegister, ArrayList<Register<Long>> lhsIdTail, ArrayList<Register<Object>> lhsObjectTail,
                      Operator lhs, Operator rhs) {
        this.joinRegister = joinRegister;
        this.lhsIdTail = lhsIdTail;
        this.lhsObjectTail = lhsObjectTail;
        this.lhs = lhs;
        this.rhs = rhs;
        this.hashTable = new ArrayList<>(1024);
    }
    private final static int hash1(int key,int hashTableSize) { return key&(hashTableSize-1); }
    private final static int hash2(int key,int hashTableSize) { return hashTableSize+((key^(key>>3))&(hashTableSize-1)); }

    private void buildHashTable() {
        int hashTableSize = 1024;
        hashTable.ensureCapacity(2*hashTableSize);
        for (int i = 0; i < 2*hashTableSize; i++)
            hashTable.add(null);

        lhs.open();
        while (lhs.next()) {
            int leftKey = (int)joinRegister.value.longValue();
            int slot1=hash1(leftKey,hashTableSize);
            int slot2=hash2(leftKey,hashTableSize);

            Entry e = hashTable.get(slot1);
            if (e == null || e.key != leftKey){
                e = hashTable.get(slot2);
            }
            if (e != null && e.key == leftKey){
                int offset = (e == hashTable.get(slot1))? slot1: slot2;
                // Append to the bucket
                e = new Entry(leftKey, lhsIdTail.size(), lhsObjectTail.size());
                e.next = hashTable.get(offset);
                hashTable.set(offset, e);
                for (int i=0; i < lhsIdTail.size(); i++)
                    e.idValues[i] = lhsIdTail.get(i).value;
                continue;
            }

            // create a new  tuple
            e = new Entry(leftKey, lhsIdTail.size(), lhsObjectTail.size());
            e.next = null;
            for (int i=0; i < lhsIdTail.size(); i++)
                e.idValues[i] = lhsIdTail.get(i).value;
            // insert it into the table
            insert(e);
            hashTableSize  = hashTable.size() / 2;
        }
    }

    private void insert(Entry e){
        // Try to insert
        boolean firstTable=true;
        int hashTableSize = hashTable.size() / 2;
        for (int index = 0; index < hashTableSize; index++){
            int slot = firstTable?hash1(e.key, hashTableSize): hash2(e.key, hashTableSize);
            Entry tmp = e;
            e = hashTable.get(slot);
            hashTable.set(slot, tmp);

            if (e == null)
                return;
            firstTable = !firstTable;
        }

        // Need to rehash
        ArrayList<Entry> oldTable =  new ArrayList<>(4 * hashTableSize);
        oldTable.ensureCapacity(4 * hashTableSize);
        for (int i = 0; i < 4*hashTableSize; i++)
            oldTable.add(null);
        ArrayList<Entry> tmp  = hashTable;
        hashTable = oldTable;
        oldTable = tmp;
        for (Entry entry: oldTable) {
            if (entry != null)
                insert(entry);
        }
        insert(e);
    }

    Entry lookup(int key){
        int hashTableSize = hashTable.size() / 2;
        Entry e = hashTable.get(hash1(key,hashTableSize));
        if (e != null && e.key == key)
            return e;
        e = hashTable.get(hash2(key, hashTableSize));
        if (e != null && e.key == key)
            return e;
        return null;
    }

    @Override
    public void open() {
        lhs.open();
        rhs.open();
        buildHashTable();
    }

    @Override
    public boolean next() {

        // until we find a match
        while (true){
            if (hashTableIter != null){
                joinRegister.value = new Long(hashTableIter.key);
                for (int i = 0; i < lhsIdTail.size(); i++)
                    lhsIdTail.get(i).value = hashTableIter.idValues[i];
                hashTableIter = hashTableIter.next;
                return true;
            }

            // get the next value from the right
            if (!rhs.next())
                return false;

            hashTableIter = lookup((int)joinRegister.value.longValue());
        }
    }

    @Override
    public void close() {
        hashTable.clear();
        lhs.close();
        rhs.close();
    }
}
