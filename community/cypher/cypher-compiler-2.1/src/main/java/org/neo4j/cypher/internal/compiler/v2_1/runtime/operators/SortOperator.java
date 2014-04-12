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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.oracle.truffle.api.CompilerAsserts;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameUtil;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.NodeInfo;

import org.neo4j.cypher.internal.compiler.v2_1.runtime.Operator;

@NodeInfo(shortName = "sort")
public class SortOperator extends Operator
{
    @Child
    private Operator left;

    private final int[] sortSlots;

    private final FrameSlot iteratorSlot;

    public SortOperator( Operator left, FrameSlot[] sortSlots, FrameSlot iteratorSlot ) {
        this.left = left;

        List<FrameSlot> leftSlots = Arrays.asList(left.getSlotsWrittenTo());
        int[] array = new int[sortSlots.length];
        for (int i = 0; i < sortSlots.length; ++i) {
            array[i] = leftSlots.indexOf( sortSlots[i] );
        }
        this.sortSlots = array;
        this.iteratorSlot = iteratorSlot;
    }

    @Override
    public FrameSlot[] getSlotsWrittenTo()
    {
        return left.getSlotsWrittenTo();
    }

    @ExplodeLoop
    private long[] getLeftValues( VirtualFrame frame ) {
        FrameSlot[] slots = this.left.getSlotsWrittenTo();
        CompilerAsserts.compilationConstant( slots.length );
        long[] row = new long[slots.length];
        for (int i = 0; i < slots.length; ++i) {
            FrameSlot slot = slots[i];
            switch (slot.getKind()) {
                case Long:
                    row[i++] = FrameUtil.getLongSafe( frame, slot );
                    break;
                case Int:
                    row[i++] = (long) FrameUtil.getIntSafe( frame, slot );
                    break;
                case Boolean:
                    row[i++] = FrameUtil.getBooleanSafe( frame, slot ) ? 1 : 0;
                    break;
                default:
                    throw new RuntimeException( "unexpected slot type" );
            }
        }
        return row;
    }

    @ExplodeLoop
    private void putLeftValues( VirtualFrame frame, long[] values ) {
        FrameSlot[] slots = this.left.getSlotsWrittenTo();
        CompilerAsserts.compilationConstant(slots.length);
        for (int i = 0; i < slots.length; ++i) {
            FrameSlot slot = slots[i];
            long value = values[i];
            switch (slot.getKind()) {
                case Long:
                    frame.setLong( slot, value );
                    break;
                case Int:
                    frame.setInt( slot, (int) value );
                    break;
                case Boolean:
                    frame.setBoolean( slot, value > 0 );
                    break;
                default:
                    throw new RuntimeException( "unexpected slot type" );
            }
        }
    }

    @Override
    public void open( VirtualFrame frame )
    {
        frame.setObject( iteratorSlot, null );

        left.open( frame );

        List<long[]> values = new ArrayList();
        while (left.next( frame )) {
            values.add(getLeftValues(frame));
        }
        long[][] rows = values.toArray( new long[][]{} );
        Arrays.sort( rows, new Comparator<long[]>() {
            @Override
            public int compare( long[] o1, long[] o2 )
            {
                for (int sortSlot: sortSlots) {
                    if (o1[sortSlot] == o2[sortSlot])
                        continue;
                    return o1[sortSlot] < o2[sortSlot] ? -1 : 1;
                }
                return 0;
            }
        });
        values = Arrays.asList( rows );

        left.close( frame );

        frame.setObject( iteratorSlot, values.iterator() );
    }

    @Override
    public boolean next( VirtualFrame frame )
    {
        Iterator<long[]> it = (Iterator<long[]>) FrameUtil.getObjectSafe( frame, iteratorSlot );
        if (it.hasNext()) {
            putLeftValues( frame, it.next() );
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void close( VirtualFrame frame )
    {
        frame.setObject( iteratorSlot, null );
    }
}
