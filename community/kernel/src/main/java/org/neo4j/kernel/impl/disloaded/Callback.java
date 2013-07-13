package org.neo4j.kernel.impl.disloaded;

public interface Callback<T>
{
    public void callMe( T node );
}
