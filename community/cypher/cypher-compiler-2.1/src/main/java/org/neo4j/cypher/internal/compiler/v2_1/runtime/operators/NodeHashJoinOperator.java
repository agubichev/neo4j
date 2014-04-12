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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.oracle.truffle.api.CompilerAsserts;
import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameUtil;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.NodeInfo;

import org.neo4j.cypher.internal.compiler.v2_1.runtime.Operator;
import org.neo4j.helpers.collection.IteratorUtil;

@NodeInfo(shortName = "nodeHashJoin")
public class NodeHashJoinOperator extends Operator
{
    @Child
    private Operator left;

    @Child
    private Operator right;

    private final FrameSlot joinSlot;
    private final FrameSlot mapSlot;
    private final FrameSlot iteratorSlot;

    private final FrameSlot[] slots;

    public NodeHashJoinOperator( Operator left, Operator right, FrameSlot joinSlot, FrameSlot mapSlot, FrameSlot iteratorSlot )
    {
        this.left = left;
        this.right = right;
        this.joinSlot = joinSlot;
        this.mapSlot = mapSlot;
        this.iteratorSlot = iteratorSlot;
        ArrayList<FrameSlot> slots = new ArrayList<>( Arrays.asList( left.getSlotsWrittenTo() ));
        for (FrameSlot slot: right.getSlotsWrittenTo()) {
            if (slots.contains( slot )) {
                slots.add( slot );
            }
        }
        this.slots = slots.toArray( new FrameSlot[slots.size()] );
    }

    @Override
    public FrameSlot[] getSlotsWrittenTo()
    {
        return slots;
    }

    @ExplodeLoop
    private long[] getLeftValues( VirtualFrame frame ) {
        CompilerAsserts.compilationConstant(slots.length);

        FrameSlot[] slots = this.left.getSlotsWrittenTo();
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
        CompilerAsserts.compilationConstant(slots.length);

        FrameSlot[] slots = this.left.getSlotsWrittenTo();
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
    @CompilerDirectives.SlowPath
    public void open( VirtualFrame frame )
    {
        Map<Long, ArrayList<long[]>> map = new HashMap<>();
        //LongKeyObjectValueTable<ArrayList<long[]>> table = new LongKeyObjectValueTable<>(1);
        //PrimitiveLongObjectHashMap<ArrayList<long[]>> map = new PrimitiveLongObjectHashMap<>( table, HopScotchHashingAlgorithm.NO_MONITOR );
        left.open( frame );
        while (left.next( frame )) {
            long joinNodeId = FrameUtil.getLongSafe( frame, joinSlot );
            ArrayList<long[]> values;
            if (map.containsKey( joinNodeId )) {
                values = map.get( joinNodeId );
            } else {
                values = new ArrayList<>();
                map.put( joinNodeId, values );
            }
            values.add(getLeftValues(frame));
        }
        if ( CompilerDirectives.inInterpreter()) {
            getRootNode().reportLoopCount( map.size() );
        }
        left.close( frame );

        frame.setObject(mapSlot, map );
        frame.setObject( iteratorSlot, IteratorUtil.emptyIterator() );
        right.open( frame );
    }

    @Override
    public boolean next( VirtualFrame frame )
    {
        Map<Long, ArrayList<long[]>> map = (Map<Long, ArrayList<long[]>>) FrameUtil.getObjectSafe( frame, mapSlot);
        //PrimitiveLongObjectHashMap<ArrayList<long[]>> map = (PrimitiveLongObjectHashMap<ArrayList<long[]>>)
        Iterator<long[]> it = (Iterator<long[]>) FrameUtil.getObjectSafe( frame, iteratorSlot );
        while (!it.hasNext()) {
            if (!right.next( frame )) {
                return false;
            }
            long joinNodeId = FrameUtil.getLongSafe( frame, joinSlot );
            if (map.containsKey( joinNodeId )) {
                it = map.get( joinNodeId ).iterator();
            } else {
                it = IteratorUtil.emptyIterator();
            }
            frame.setObject(iteratorSlot, it);
        }

        putLeftValues( frame, it.next() );
        return true;
    }

    @Override
    public void close( VirtualFrame frame )
    {
        right.close( frame );
    }
}
