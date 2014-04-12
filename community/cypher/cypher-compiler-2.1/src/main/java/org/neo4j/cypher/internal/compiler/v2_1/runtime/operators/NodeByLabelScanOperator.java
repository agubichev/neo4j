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

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cypher.internal.compiler.v2_1.runtime.Expression;
import org.neo4j.cypher.internal.compiler.v2_1.runtime.Operator;

@NodeInfo(shortName = "nodeByLabelScan")
public class NodeByLabelScanOperator extends Operator
{
    private final Expression labelId;

    private final FrameSlot iteratorSlot;
    private final FrameSlot slot;
    private final FrameSlot[] slots;

    public NodeByLabelScanOperator( Expression labelId, FrameSlot slot, FrameSlot iteratorSlot )
    {
        this.labelId = labelId;
        this.slot = slot;
        this.iteratorSlot = iteratorSlot;
        this.slots = new FrameSlot[] { slot };
    }

    @Override
    public FrameSlot[] getSlotsWrittenTo()
    {
        return slots;
    }

    @Override
    public void open( VirtualFrame frame )
    {
        PrimitiveLongIterator it = getStatement( frame ).readOperations().nodesGetForLabel( labelId.executeToIntegerSafe( frame ) );
        frame.setObject( iteratorSlot, it );
    }

    @Override
    public boolean next( VirtualFrame frame )
    {
        PrimitiveLongIterator it = (PrimitiveLongIterator) FrameUtil.getObjectSafe( frame, iteratorSlot );
        if ( it.hasNext() )
        {
            frame.setLong( slot, it.next() );
            return true;
        }
        return false;
    }

    @Override
    public void close( VirtualFrame frame )
    {
    }
}
