package org.neo4j.cypher.internal.compilator.v2_0;

import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;

public interface Expression<T> {
    public T execute(Register register, Statement statement) throws EntityNotFoundException, PropertyNotFoundException;
}
