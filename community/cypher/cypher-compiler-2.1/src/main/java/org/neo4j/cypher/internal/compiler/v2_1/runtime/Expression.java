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

import com.oracle.truffle.api.dsl.TypeSystemReference;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;

@TypeSystemReference(CypherTypeSystem.class)
public abstract class Expression extends Node
{
    public boolean executeToBoolean( VirtualFrame frame ) throws UnexpectedResultException
    {
        return CypherTypeSystemGen.CYPHERTYPESYSTEM.expectBoolean( execute( frame ) );
    }

    public boolean executeToBooleanSafe( VirtualFrame frame )
    {
        try
        {
            return executeToBoolean( frame );
        }
        catch ( UnexpectedResultException e )
        {
            throw new RuntimeException( e );
        }
    }

    public long executeToLong( VirtualFrame frame ) throws UnexpectedResultException
    {
        return CypherTypeSystemGen.CYPHERTYPESYSTEM.expectLong( execute( frame ) );
    }

    public long executeToLongSafe( VirtualFrame frame )
    {
        try
        {
            return executeToLong( frame );
        }
        catch ( UnexpectedResultException e )
        {
            throw new RuntimeException( e );
        }
    }

    public int executeToInteger( VirtualFrame frame ) throws UnexpectedResultException
    {
        return CypherTypeSystemGen.CYPHERTYPESYSTEM.expectInteger( execute( frame ) );
    }

    public int executeToIntegerSafe( VirtualFrame frame )
    {
        try
        {
            return executeToInteger( frame );
        }
        catch ( UnexpectedResultException e )
        {
            throw new RuntimeException( e );
        }
    }

    public String executeToString( VirtualFrame frame ) throws UnexpectedResultException
    {
        return CypherTypeSystemGen.CYPHERTYPESYSTEM.expectString( execute( frame ) );
    }

    public String executeToStringSafe( VirtualFrame frame )
    {
        try
        {
            return executeToString( frame );
        }
        catch ( UnexpectedResultException e )
        {
            throw new RuntimeException( e );
        }
    }

    public void executeAndIgnore( VirtualFrame frame )
    {
        execute( frame );
    }

    public abstract Object execute( VirtualFrame frame );
}
