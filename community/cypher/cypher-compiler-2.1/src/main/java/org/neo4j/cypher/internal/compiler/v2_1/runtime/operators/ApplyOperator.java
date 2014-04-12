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
package org.neo4j.cypher.internal.compiler.v2_1.runtime.operators;

import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameUtil;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.NodeInfo;
import org.neo4j.cypher.internal.compiler.v2_1.runtime.Operator;

import java.util.ArrayList;
import java.util.Arrays;

@NodeInfo(shortName = "apply")
public class ApplyOperator extends Operator
{
    @Child
    private Operator left;

    @Child
    private Operator inner;

    private final FrameSlot openSlot;

    private final FrameSlot[] slots;

    public ApplyOperator(Operator left, Operator inner, FrameSlot openSlot)
    {
        this.left = left;
        this.inner = inner;
        this.openSlot = openSlot;
        ArrayList<FrameSlot> slots = new ArrayList<>( Arrays.asList( left.getSlotsWrittenTo() ));
        slots.add( openSlot );
        this.slots = slots.toArray( new FrameSlot[slots.size()] );
    }

    @Override
    public FrameSlot[] getSlotsWrittenTo()
    {
        return slots;
    }

    @Override
    public void open( VirtualFrame frame )
    {
        this.left.open( frame );
        frame.setBoolean( openSlot, false );
    }

    @Override
    public boolean next( VirtualFrame frame )
    {
        while (true) {
            if (FrameUtil.getBooleanSafe( frame, openSlot )) {
                if (inner.next( frame ))
                    return true;

                inner.close( frame );
                frame.setBoolean( openSlot, false );
            }

            if (!left.next( frame ))
                return false;

            inner.open( frame );
            frame.setBoolean( openSlot, true );
        }
    }

    @Override
    public void close( VirtualFrame frame )
    {
        if (FrameUtil.getBooleanSafe( frame, openSlot )) {
            inner.close( frame );
        }
        this.left.close( frame );
    }
}
