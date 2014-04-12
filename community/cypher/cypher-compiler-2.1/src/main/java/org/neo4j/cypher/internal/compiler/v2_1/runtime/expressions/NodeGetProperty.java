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
package org.neo4j.cypher.internal.compiler.v2_1.runtime.expressions;

import com.oracle.truffle.api.dsl.Generic;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.NodeInfo;

import org.neo4j.cypher.internal.compiler.v2_1.runtime.BinaryExpression;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;

@NodeInfo(shortName = "nodeGetProperty")
public abstract class NodeGetProperty extends BinaryExpression
{
    @Specialization(rewriteOn = ClassCastException.class)
    long read(VirtualFrame frame, long id, int propertyKeyId)
    {
        try
        {
            return getStatement(frame)
                    .readOperations()
                    .nodeGetProperty(id, propertyKeyId)
                    .longValue();
        }
        catch ( EntityNotFoundException | PropertyNotFoundException e )
        {
            throw new ClassCastException();
        }
    }

    @Specialization
    Object readObject(VirtualFrame frame, long id, int propertyKeyId )
    {
        try
        {
            return getStatement( frame )
                    .readOperations()
                    .nodeGetProperty( id, propertyKeyId )
                    .value();
        }
        catch ( EntityNotFoundException | PropertyNotFoundException e )
        {
            return null;
        }
    }

    @Generic
    Object read(VirtualFrame frame, Object id, Object propertyKeyId )
    {
        assert false;
        return null;
    }
}
