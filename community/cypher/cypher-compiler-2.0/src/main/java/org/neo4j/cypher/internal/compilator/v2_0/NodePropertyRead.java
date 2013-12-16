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
import org.neo4j.kernel.api.properties.Property;

public class NodePropertyRead implements Expression<Object> {

    private final Expression<Long> nodeExpression;
    private final int propertyId;

    public NodePropertyRead(Expression<Long> nodeExpression, int propertyId) {
        this.nodeExpression = nodeExpression;
        this.propertyId = propertyId;
    }

    @Override
    public Object execute(Register register, Statement statement) throws EntityNotFoundException, PropertyNotFoundException {
        Long nodeId = nodeExpression.execute(register, statement);
        Property property = statement.readOperations().nodeGetProperty(nodeId, propertyId);
        return property.value();
    }
}
