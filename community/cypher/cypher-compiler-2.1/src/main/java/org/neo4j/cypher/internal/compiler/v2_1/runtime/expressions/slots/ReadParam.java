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
package org.neo4j.cypher.internal.compiler.v2_1.runtime.expressions.slots;

import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;

import org.neo4j.cypher.internal.compiler.v2_1.runtime.Expression;
import org.neo4j.cypher.internal.compiler.v2_1.runtime.expressions.literals.BooleanLiteral;
import org.neo4j.cypher.internal.compiler.v2_1.runtime.expressions.literals.IntegerLiteral;
import org.neo4j.cypher.internal.compiler.v2_1.runtime.expressions.literals.LongLiteral;
import org.neo4j.cypher.internal.compiler.v2_1.runtime.expressions.literals.ValueLiteral;

public class ReadParam extends Expression
{
    private final FrameSlot slot;

    public ReadParam( FrameSlot slot )
    {
        this.slot = slot;
    }

    @Override
    public long executeToLong( VirtualFrame frame ) throws UnexpectedResultException
    {
        long value = super.executeToLong( frame );
        replace( new LongLiteral( value ) );
        return value;
    }

    @Override
    public int executeToInteger( VirtualFrame frame ) throws UnexpectedResultException
    {
        int value = super.executeToInteger( frame );
        replace( new IntegerLiteral( value ) );
        return value;
    }

    @Override
    public boolean executeToBoolean( VirtualFrame frame ) throws UnexpectedResultException
    {
        boolean value = super.executeToBoolean( frame );
        replace( new BooleanLiteral( value ) );
        return value;
    }

    @Override
    public Object execute( VirtualFrame frame )
    {
        Object value = frame.getValue( slot );
        replace( new ValueLiteral( value ) );
        return value;
    }
}
