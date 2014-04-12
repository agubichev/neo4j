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

import org.neo4j.cypher.internal.compiler.v2_1.runtime.Expression;
import org.neo4j.cypher.internal.compiler.v2_1.runtime.Operator;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;

@NodeInfo(shortName = "nodeByUniqueIndexSeek")
public class NodeByUniqueIndexSeekOperator extends Operator
{
    @Child
    private Expression valueExpression;

    private final FrameSlot slot;
    private final FrameSlot consumedSlot;

    private final IndexDescriptor descriptor;

    private final FrameSlot[] slots;

    public NodeByUniqueIndexSeekOperator( int labelId, int propertyKeyId, Expression valueExpression, FrameSlot slot,
                                          FrameSlot consumedSlot )
    {
        this.descriptor = new IndexDescriptor( labelId, propertyKeyId );
        this.valueExpression = valueExpression;
        this.slot = slot;
        this.consumedSlot = consumedSlot;
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
        frame.setBoolean( consumedSlot, false );
    }

    @Override
    public boolean next( VirtualFrame frame )
    {
        if ( FrameUtil.getBooleanSafe( frame, consumedSlot ) )
        {
            return false;
        }

        ReadOperations ops = getStatement( frame ).readOperations();
        long nodeId;
        try
        {
            nodeId = ops.nodeGetUniqueFromIndexLookup( descriptor, valueExpression.execute( frame ) );
        }
        catch ( IndexNotFoundKernelException | IndexBrokenKernelException e )
        {
            throw new RuntimeException( e );
        }
        frame.setBoolean( consumedSlot, true );
        if ( nodeId == StatementConstants.NO_SUCH_NODE )
        {
            return false;
        }

        frame.setLong( slot, nodeId );
        return true;
    }

    @Override
    public void close( VirtualFrame frame )
    {
    }
}
