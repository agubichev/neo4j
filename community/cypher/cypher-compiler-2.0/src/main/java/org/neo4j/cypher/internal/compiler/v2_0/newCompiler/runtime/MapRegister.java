package org.neo4j.cypher.internal.compiler.v2_0.newCompiler.runtime;

import java.util.HashMap;

public class MapRegister implements Register {

    private final HashMap<Integer, Long> longs;
    private final HashMap<Integer, Object> objects;

    public MapRegister() {
        this(new HashMap<Integer, Long>(), new HashMap<Integer, Object>());
    }

    protected MapRegister(HashMap<Integer, Long> longs, HashMap<Integer, Object> objects) {
        this.longs = longs;
        this.objects = objects;
    }

    @Override
    public void setObject(int idx, Object value) {
        objects.put(idx, value);
    }

    @Override
    public void setLong(int idx, long value) {
        longs.put(idx, value);
    }

    @Override
    public Object getObject(int idx) {
        return objects.get(idx);
    }

    @Override
    public long getLong(int idx) {
        return longs.get(idx);
    }

    @Override
    public Register copy() {
        return new MapRegister((HashMap<Integer, Long>)longs.clone(), (HashMap<Integer, Object>)objects.clone());
    }
}
