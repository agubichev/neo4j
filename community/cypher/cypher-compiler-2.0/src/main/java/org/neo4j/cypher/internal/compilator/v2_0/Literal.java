package org.neo4j.cypher.internal.compilator.v2_0;

import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

public class Literal implements Expression<Object> {
    private final Object value;

    public Literal(Object value) {
        this.value = value;
    }

    @Override
    public Object execute(Register register, Statement statement) throws EntityNotFoundException {
        return value;
    }
}
