/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.compilator.v2_0;

import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;

public class IdxSeek implements Operation {
    private final IndexDescriptor index;
    private final Expression<Object> valueExpression;
    private final RegisterCreator registerCreator;
    private final int slotIndex;

    public IdxSeek(IndexDescriptor index, Expression<Object> valueExpression, RegisterCreator registerCreator, int slotIndex) {
        this.index = index;
        this.valueExpression = valueExpression;
        this.registerCreator = registerCreator;
        this.slotIndex = slotIndex;
    }

    @Override
    public void accept(Visitor visitor, Statement statement) throws Exception {
        Register register = registerCreator.create();

        Object value = valueExpression.execute(register, statement);
        PrimitiveLongIterator result = statement.readOperations().nodesGetFromIndexLookup(index, value);
        while (result.hasNext()) {
            register.setNode(slotIndex, result.next());
            visitor.visit(register);
        }
    }
}
