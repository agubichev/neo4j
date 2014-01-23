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
