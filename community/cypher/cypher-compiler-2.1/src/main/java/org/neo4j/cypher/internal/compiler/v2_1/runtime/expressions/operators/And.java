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
package org.neo4j.cypher.internal.compiler.v2_1.runtime.expressions.operators;

import com.oracle.truffle.api.dsl.Generic;
import com.oracle.truffle.api.dsl.ShortCircuit;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;

import org.neo4j.cypher.internal.compiler.v2_1.runtime.BinaryExpression;

@NodeInfo(shortName = "&&")
public abstract class And extends BinaryExpression
{
    @ShortCircuit("right")
    protected boolean needsRight( boolean left )
    {
        return left;
    }

    @Specialization
    protected boolean and( boolean left, boolean hasRight, boolean right )
    {
        return left && right;
    }

    @ShortCircuit("right")
    protected boolean needsRight( Object left )
    {
        return false;
    }

    @Generic
    protected boolean equals( Object left, boolean hasRight, Object right )
    {
        assert false;
        return false;
    }
}
