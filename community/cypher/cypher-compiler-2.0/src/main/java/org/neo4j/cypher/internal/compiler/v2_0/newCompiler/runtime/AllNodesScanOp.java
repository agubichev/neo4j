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
package org.neo4j.cypher.internal.compiler.v2_0.newCompiler.runtime;


import org.neo4j.kernel.impl.util.PrimitiveLongIterator;

public class AllNodesScanOp implements Operator {

    private final PrimitiveLongIterator allNodes;
    private final Register register;
    private final int dstIdx;

    public AllNodesScanOp(StatementContext ctx, Register register, int dstIdx) {
        this.register = register;
        this.dstIdx = dstIdx;
        allNodes = ctx.FAKEgetAllNodes();
    }

    @Override
    public void open() {
    }

    @Override
    public boolean next() {
        if (!allNodes.hasNext())
            return false;

        register.setLong(dstIdx, allNodes.next());

        return true;
    }

    @Override
    public void close() {
    }
}
