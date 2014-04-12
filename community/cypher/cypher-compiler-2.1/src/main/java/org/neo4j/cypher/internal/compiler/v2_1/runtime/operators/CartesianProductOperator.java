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

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.NodeInfo;
import org.neo4j.cypher.internal.compiler.v2_1.runtime.Operator;

import java.util.ArrayList;
import java.util.Arrays;

@NodeInfo(shortName = "cartesianProduct")
public class CartesianProductOperator extends Operator
{
    @Child
    private Operator left;

    @Child
    private Operator right;

    private final FrameSlot[] slots;

    public CartesianProductOperator(Operator left, Operator right)
    {
        this.left = left;
        this.right = right;
        ArrayList<FrameSlot> slots = new ArrayList<>( Arrays.asList( left.getSlotsWrittenTo() ));
        slots.addAll( Arrays.asList(right.getSlotsWrittenTo()) );
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
        this.right.open( frame );
        this.left.next( frame );
    }

    @Override
    public boolean next( VirtualFrame frame )
    {
        int count = 0;
        while (!this.right.next( frame )) {
            if ( CompilerDirectives.inInterpreter()) {
                ++count;
            }
            this.right.close( frame );
            if (!this.left.next( frame )) {
                return false;
            }
            this.right.open( frame );
        }
        if ( CompilerDirectives.inInterpreter()) {
            getRootNode().reportLoopCount( count );
        }
        return true;
    }

    @Override
    public void close( VirtualFrame frame )
    {
        this.left.close( frame );
        this.right.close( frame );
    }
}
