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

public class Projection implements Operation {
    private final Operation source;
    private final int slotIndex;
    private final Expression<Object> expression;

    public Projection(Operation source, int slotIndex, Expression<Object> expression) {
        this.source = source;
        this.slotIndex = slotIndex;
        this.expression = expression;
    }

    @Override
    public void accept(Visitor visitor, Statement statement) throws Exception {
        ProjectionVisitor projectionVisitor = new ProjectionVisitor(visitor, statement);
        source.accept(projectionVisitor, statement);
    }

    class ProjectionVisitor implements Visitor {
        private final Visitor inner;
        private final Statement statement;

        ProjectionVisitor(Visitor inner, Statement statement) {
            this.inner = inner;
            this.statement = statement;
        }

        @Override
        public void visit(Register register) throws Exception {
            Object value = expression.execute(register, statement);
            register.setObject(slotIndex, value);
            inner.visit(register);
        }
    }
}
