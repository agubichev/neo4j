package org.neo4j.cypher.internal.compiler.v2_0.newCompiler.runtime;

import java.util.Arrays;

public class ArrayRegisters implements Registers
{
    private final Object[] objects;
    private final long[] longs;

    public ArrayRegisters(int numObjects, int numLongs)
    {
        this( new Object[numObjects], new long[numLongs] );
    }

    public ArrayRegisters(Object[] objects, long[] longs)
    {
        this.objects = objects;
        this.longs = longs;
    }

    @Override
    public void setObjectRegister( int idx, Object value )
    {
        objects[idx] = value;
    }

    @Override
    public void setLongRegister( int idx, long value )
    {
        longs[idx] = value;
    }

    @Override
    public Object getObjectRegister( int idx )
    {
        return objects[idx];
    }

    @Override
    public long getLongRegister( int idx )
    {
        return longs[idx];
    }

    @Override
    public Registers copy()
    {
        return new ArrayRegisters( Arrays.copyOf( objects, objects.length ), Arrays.copyOf( longs, longs.length ) );
    }
}
