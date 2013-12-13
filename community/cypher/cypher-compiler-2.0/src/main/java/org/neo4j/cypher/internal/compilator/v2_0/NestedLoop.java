package org.neo4j.cypher.internal.compilator.v2_0;

import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;

public class NestedLoop implements Operation {
    private Operation left;
    private Operation right;

    public NestedLoop(Operation left, Operation right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public void accept(Visitor visitor, Statement statement) throws Exception {
        JoinLeftVisitor projectionVisitor = new JoinLeftVisitor(visitor, statement, right);
        left.accept(projectionVisitor, statement);
    }

    class JoinLeftVisitor implements Visitor {
        private final Visitor inner;
        private final Statement statement;
        private final RightVisitor rightVisitor;
        private final Operation right;

        JoinLeftVisitor(Visitor inner, Statement statement, Operation right) {
            this.inner = inner;
            this.statement = statement;
            this.right = right;
            this.rightVisitor = new RightVisitor();
        }

        @Override
        public void visit(Register register) throws Exception {
            right.accept(rightVisitor, statement);
        }

        class RightVisitor implements Visitor {
            @Override
            public void visit(Register register) throws Exception {
                inner.visit(register);
            }
        }
    }
}
