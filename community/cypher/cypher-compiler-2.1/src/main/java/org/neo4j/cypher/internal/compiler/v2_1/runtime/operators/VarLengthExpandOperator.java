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
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Stack;

@NodeInfo(shortName = "varLengthExpand")
public class VarLengthExpandOperator extends Operator
{
    private final int min;
    private final Integer max;

    @Child
    private Operator left;

    @Child
    private Expression relTypeId;

    private final FrameSlot fromSlot;
    private final FrameSlot relSlot;
    private final FrameSlot toSlot;
    private final FrameSlot stackSlot;
    private final FrameSlot iteratorSlot;

    private final FrameSlot[] slots;

    private final Direction direction;

    public VarLengthExpandOperator( Operator left, int min, Integer max,
                                    Expression relTypeId, FrameSlot fromSlot, FrameSlot relSlot,
                                    FrameSlot toSlot, FrameSlot stackSlot,
                                    FrameSlot iteratorSlot, Direction direction )
    {
        this.left = left;
        this.min = min;
        this.max = max;
        this.relTypeId = relTypeId;
        this.fromSlot = fromSlot;
        this.relSlot = relSlot;
        this.toSlot = toSlot;
        this.stackSlot = stackSlot;
        this.iteratorSlot = iteratorSlot;
        this.direction = direction;
        ArrayList<FrameSlot> slots = new ArrayList<>( Arrays.asList(left.getSlotsWrittenTo()));
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
        frame.setObject( stackSlot, new Stack<long[]>() );
        frame.setObject( iteratorSlot, null );
    }

    @Override
    public boolean next( VirtualFrame frame )
    {
        GraphDatabaseService graph = getGraph( frame );
        Statement statement = getStatement( frame );
        int typeId = relTypeId.executeToIntegerSafe( frame );

        Stack<long[]> stack = (Stack<long[]>) FrameUtil.getObjectSafe(frame, stackSlot);
        long[] rels = {};
        while (rels.length - 1 < min) {
            if (stack.isEmpty()) {
                if (!left.next( frame )) {
                    return false;
                }
                long nodeId = FrameUtil.getLongSafe( frame, fromSlot );
                stack.push(new long[] { nodeId });
            }

            rels = stack.pop();
            if (max == null || rels.length - 1 < max) {
                PrimitiveLongIterator it;
                try
                {
                    it = statement.readOperations().nodeGetRelationships(
                            rels[0], direction, typeId
                    );
                }
                catch ( EntityNotFoundException e )
                {
                    throw new RuntimeException( e );
                }

                while (it.hasNext()) {
                    long relId = it.next();
                    // I can't believe this.
                    boolean contains = false;
                    for (int i = 1; i < rels.length; ++i) {
                        if (rels[i] == relId) {
                            contains = true;
                            break;
                        }
                    }
                    if (contains) {
                        continue;
                    }

                    long[] newRels = Arrays.copyOf( rels, rels.length + 1 );
                    newRels[rels.length] = relId;

                    long newLastNodeId;
                    if (direction == Direction.OUTGOING) {
                        newLastNodeId = graph.getRelationshipById( relId ).getEndNode().getId();
                    } else {
                        newLastNodeId = graph.getRelationshipById( relId ).getStartNode().getId();
                    }
                    newRels[0] = newLastNodeId;
                    stack.push(newRels);
                }
            }
        }

        frame.setObject( relSlot, Arrays.copyOfRange( rels, 1, rels.length ) );
        frame.setLong( toSlot, rels[0] );
        return true;
    }

    @Override
    public void close( VirtualFrame frame )
    {
        this.left.close( frame );
    }
}
