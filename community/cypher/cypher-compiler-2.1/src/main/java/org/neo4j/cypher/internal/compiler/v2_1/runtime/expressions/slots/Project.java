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
package org.neo4j.cypher.internal.compiler.v2_1.runtime.expressions.slots;

import com.oracle.truffle.api.dsl.NodeField;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.FrameUtil;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.NodeInfo;

import org.neo4j.cypher.internal.compiler.v2_1.runtime.Expression;

@NodeInfo(shortName = "project")
@NodeField(name = "slot", type = FrameSlot.class)
public abstract class Project extends Expression
{
    protected abstract FrameSlot getSlot();

    @Specialization(order = 1, guards = {"isNodeSlot", "isOptionalSlot"})
    protected Object readOptionalNode( VirtualFrame frame )
    {
        Long id = (Long) FrameUtil.getObjectSafe( frame, getSlot() );
        return id == null ? null : getGraph( frame ).getNodeById( id );
    }

    @Specialization(order = 2, guards = "isNodeSlot")
    protected Object readNode( VirtualFrame frame )
    {
        return getGraph( frame ).getNodeById( FrameUtil.getLongSafe( frame, getSlot() ) );
    }

    @Specialization(order = 3, guards = {"isRelationshipSlot", "isOptionalSlot"})
    protected Object readOptionalRelationship( VirtualFrame frame )
    {
        Long id = (Long) FrameUtil.getObjectSafe( frame, getSlot() );
        return id == null ? null : getGraph( frame ).getRelationshipById( id );
    }

    @Specialization(order = 4, guards = "isRelationshipSlot")
    protected Object readRelationship( VirtualFrame frame )
    {
        return getGraph( frame ).getRelationshipById(FrameUtil.getLongSafe(frame, getSlot()));
    }

    @Specialization(order = 5)
    protected Object read( VirtualFrame frame )
    {
        return frame.getValue( getSlot() );
    }

    protected final boolean isOptionalSlot() {
        FrameSlotKind kind = getSlot().getKind();
        return kind == FrameSlotKind.Object;
    }

    protected final boolean isNodeSlot() {
        String identifier = (String) getSlot().getIdentifier();
        return identifier.startsWith( "__node__" );
    }

    protected final boolean isRelationshipSlot() {
        String identifier = (String) getSlot().getIdentifier();
        return identifier.startsWith( "__rel__" );
    }
}
