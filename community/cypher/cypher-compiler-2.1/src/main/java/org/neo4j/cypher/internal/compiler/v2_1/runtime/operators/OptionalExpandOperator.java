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
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cypher.internal.compiler.v2_1.runtime.Expression;
import org.neo4j.cypher.internal.compiler.v2_1.runtime.Operator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

import java.util.ArrayList;
import java.util.Arrays;

@NodeInfo(shortName = "optionalExpand")
public class OptionalExpandOperator extends Operator
{
    @Child
    private Operator left;

    @Child
    private Expression relTypeId;

    @Child
    private Expression predicate;

    private final FrameSlot fromSlot;
    private final FrameSlot relSlot;
    private final FrameSlot toSlot;
    private final FrameSlot iteratorSlot;

    private final FrameSlot[] slots;

    private final Direction direction;

    public OptionalExpandOperator(Operator left, Expression relTypeId, FrameSlot fromSlot, FrameSlot toSlot,
                                  FrameSlot relSlot, FrameSlot iteratorSlot, Direction direction, Expression predicate)
    {
        this.left = left;
        this.relTypeId = relTypeId;
        this.fromSlot = fromSlot;
        this.relSlot = relSlot;
        this.toSlot = toSlot;
        this.iteratorSlot = iteratorSlot;
        this.direction = direction;
        this.predicate = predicate;
        ArrayList<FrameSlot> slots = new ArrayList<>( Arrays.asList( left.getSlotsWrittenTo() ));
        slots.add( relSlot );
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
        frame.setObject( iteratorSlot, PrimitiveLongCollections.emptyIterator() );
    }

    void getNextRelationship( VirtualFrame frame, long relId ) {
        GraphDatabaseService graph = getGraph( frame );
        frame.setObject( relSlot, relId );
        Relationship relationship = graph.getRelationshipById( relId );
        long endNodeId;
        if ( direction == Direction.INCOMING ) {
            endNodeId = relationship.getStartNode().getId();
        } else {
            endNodeId = relationship.getEndNode().getId();
        }
        frame.setObject( toSlot, endNodeId );
    }

    boolean getNext( VirtualFrame frame ) {
        PrimitiveLongIterator it = (PrimitiveLongIterator) FrameUtil.getObjectSafe( frame, iteratorSlot );
        while (it.hasNext()) {
            long relId = it.next();
            getNextRelationship( frame, relId );
            if (predicate.executeToBooleanSafe(frame)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean next( VirtualFrame frame )
    {
        Statement statement = getStatement( frame );
        int typeId = relTypeId.executeToIntegerSafe( frame );

        while (!getNext(frame)) {
            if (!left.next( frame )) {
                return false;
            }

            long nodeId = FrameUtil.getLongSafe( frame, fromSlot );
            try
            {
                PrimitiveLongIterator it = statement.readOperations().nodeGetRelationships( nodeId, direction, typeId );
                frame.setObject( iteratorSlot, it );
                if (!getNext( frame )) {
                    frame.setObject( relSlot, null );
                    frame.setObject( toSlot, null );
                }
                return true;
            }
            catch ( EntityNotFoundException e )
            {
                throw new RuntimeException( e );
            }
        }
        return true;
    }

    @Override
    public void close( VirtualFrame frame )
    {
        this.left.close( frame );
    }
}
