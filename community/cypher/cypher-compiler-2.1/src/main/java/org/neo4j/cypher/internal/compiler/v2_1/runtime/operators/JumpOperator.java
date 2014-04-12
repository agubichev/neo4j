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

import com.oracle.truffle.api.frame.*;
import com.oracle.truffle.api.nodes.NodeInfo;
import org.neo4j.cypher.internal.compiler.v2_1.runtime.Operator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;
import java.util.Arrays;

@NodeInfo(shortName = "jump")
public class JumpOperator extends Operator
{
    @Child
    private Operator left;

    private final FrameSlot toSlot;
    private final FrameSlot relSlot;

    private final Direction direction;

    private final FrameSlot[] slots;

    public JumpOperator( Operator left, FrameSlot relSlot, FrameSlot toSlot, Direction direction )
    {
        this.left = left;
        this.toSlot = toSlot;
        this.relSlot = relSlot;
        this.direction = direction;
        ArrayList<FrameSlot> slots = new ArrayList<>( Arrays.asList( left.getSlotsWrittenTo() ));
        slots.add( toSlot );
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
    }

    @Override
    public boolean next( VirtualFrame frame )
    {
        GraphDatabaseService graph = getGraph( frame );
        if (!left.next( frame )) {
            return false;
        }
        long relId = FrameUtil.getLongSafe( frame, relSlot );
        Relationship relationship = graph.getRelationshipById(relId);
        long endNodeId;
        if ( direction == Direction.INCOMING ) {
            endNodeId = relationship.getStartNode().getId();
        } else {
            endNodeId = relationship.getEndNode().getId();
        }
        frame.setLong( toSlot, endNodeId );
        return true;
    }

    @Override
    public void close( VirtualFrame frame )
    {
        this.left.close( frame );
    }
}
