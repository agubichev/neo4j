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
