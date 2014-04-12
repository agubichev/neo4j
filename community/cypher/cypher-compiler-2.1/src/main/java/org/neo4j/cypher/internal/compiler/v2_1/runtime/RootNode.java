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
package org.neo4j.cypher.internal.compiler.v2_1.runtime;

import java.util.ArrayList;

import com.oracle.truffle.api.CompilerAsserts;
import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.NodeInfo;
import com.oracle.truffle.api.nodes.NodeUtil;

@NodeInfo(shortName = "root")
public class RootNode extends com.oracle.truffle.api.nodes.RootNode {
    @Child
    private Operator operator;

    private final Operator unitializedOperator;

    private final FrameSlot projection;

    private final int rowCapacity;

    private final FrameSlot[] paramSlots;

    public RootNode(Operator operator, FrameDescriptor descriptor, int rowCapacity, FrameSlot[] paramSlots) {
        super(null, descriptor);
        this.operator = operator;
        this.unitializedOperator = NodeUtil.cloneNode(operator);
        this.projection = descriptor.findFrameSlot( "projection" );
        this.rowCapacity = rowCapacity;
        this.paramSlots = paramSlots;
    }

    @ExplodeLoop
    public void fillParamSlots( VirtualFrame frame )
    {
        Object[] params = (Object[]) frame.getArguments()[2];
        CompilerAsserts.compilationConstant( paramSlots.length );

        for (int i = 0; i < paramSlots.length; ++i) {
            FrameSlot slot = paramSlots[i];
            Object value = params[i];
            frame.setObject( slot, value );
        }
    }

    @Override
    public Object execute( VirtualFrame frame )
    {
        fillParamSlots( frame );

        ArrayList<Object> result = new ArrayList<>();
        operator.open( frame );

        ArrayList<Object> list = new ArrayList<>( rowCapacity );
        frame.setObject( projection, list );

        while (operator.next( frame )) {
            result.add( list.toArray() );
            list.clear();
        }
        if ( CompilerDirectives.inInterpreter()) {
            reportLoopCount( result.size() );
        }
        operator.close( frame );
        return result.toArray();
    }

    @Override
    public boolean isSplittable() {
        return true;
    }

    @Override
    public RootNode split() {
        return new RootNode(NodeUtil.cloneNode( unitializedOperator ), getFrameDescriptor().shallowCopy(), rowCapacity, paramSlots);
    }
}
