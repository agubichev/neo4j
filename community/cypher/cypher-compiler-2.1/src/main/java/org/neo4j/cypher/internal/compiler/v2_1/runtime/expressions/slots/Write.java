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

import com.oracle.truffle.api.dsl.Generic;
import com.oracle.truffle.api.dsl.NodeField;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotTypeException;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.NodeInfo;

import org.neo4j.cypher.internal.compiler.v2_1.runtime.UnaryExpression;

@NodeInfo(shortName = "write")
@NodeField(name = "slot", type = FrameSlot.class)
public abstract class Write extends UnaryExpression
{
    protected abstract FrameSlot getSlot();

    public FrameSlot writesTo() {
        return getSlot();
    }

    @Specialization(rewriteOn = FrameSlotTypeException.class)
    protected long writeLong( VirtualFrame frame, long value ) throws FrameSlotTypeException
    {
        frame.setLong( getSlot(), value );
        return value;
    }

    @Specialization(rewriteOn = FrameSlotTypeException.class)
    protected int writeInteger( VirtualFrame frame, int value ) throws FrameSlotTypeException
    {
        frame.setInt( getSlot(), value );
        return value;
    }

    @Specialization(rewriteOn = FrameSlotTypeException.class)
    protected boolean writeBoolean( VirtualFrame frame, boolean value ) throws FrameSlotTypeException
    {
        frame.setBoolean( getSlot(), value );
        return value;
    }

    @Generic
    protected Object write( VirtualFrame frame, Object value )
    {
        frame.setObject( getSlot(), value );
        return value;
    }
}
