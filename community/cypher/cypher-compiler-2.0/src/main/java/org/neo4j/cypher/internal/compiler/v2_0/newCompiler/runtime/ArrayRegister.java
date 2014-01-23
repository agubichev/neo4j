package org.neo4j.cypher.internal.compiler.v2_0.newCompiler.runtime;

import java.util.Arrays;

public class ArrayRegister implements Register
{
    private final Object[] objects;
    private final long[] longs;

    public ArrayRegister( int numObjects, int numLongs )
    {
        this( new Object[numObjects], new long[numLongs] );
    }

    public ArrayRegister( Object[] objects, long[] longs )
    {
        this.objects = objects;
        this.longs = longs;
    }

    @Override
    public void setObject( int idx, Object value )
    {
        objects[idx] = value;
    }

    @Override
    public void setLong( int idx, long value )
    {
        longs[idx] = value;
    }

    @Override
    public Object getObject( int idx )
    {
        return objects[idx];
    }

    @Override
    public long getLong( int idx )
    {
        return longs[idx];
    }

    @Override
    public Register copy()
    {
        return new ArrayRegister( Arrays.copyOf( objects, objects.length ), Arrays.copyOf( longs, longs.length ) );
    }
}
