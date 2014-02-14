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

import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;

public class ExpandToNodeOp implements Operator {
    private final StatementContext ctx;
    private final Operator sourceOp;
    private final Register<Long> registerSource, registerDest;
    private final Direction dir;
    private PrimitiveLongIterator currentNodes;

    public ExpandToNodeOp(StatementContext ctx, Operator sourceOp, Register<Long> registerSource, Register<Long> registerDest, Direction dir) {
        this.ctx = ctx;
        this.sourceOp = sourceOp;
        this.registerSource = registerSource;
        this.registerDest = registerDest;
        this.dir = dir;
        this.currentNodes = IteratorUtil.emptyPrimitiveLongIterator();
    }

    @Override
    public void open() {
        sourceOp.open();
    }

    @Override
    public boolean next() {
        while (!currentNodes.hasNext() && sourceOp.next()) {
            currentNodes = ctx.FAKEgetNodesRelatedBy(registerSource.value, dir);
        }

        if (!currentNodes.hasNext())
            return false;

        registerDest.value = currentNodes.next();
        registerDest.bound = true;

        return true;
    }

    @Override
    public void close() {
        sourceOp.close();
    }
}
