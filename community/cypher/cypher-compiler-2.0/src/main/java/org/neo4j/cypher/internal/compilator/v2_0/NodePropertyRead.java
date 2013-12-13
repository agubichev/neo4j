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
