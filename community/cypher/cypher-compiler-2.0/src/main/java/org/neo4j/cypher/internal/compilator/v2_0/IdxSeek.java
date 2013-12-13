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
