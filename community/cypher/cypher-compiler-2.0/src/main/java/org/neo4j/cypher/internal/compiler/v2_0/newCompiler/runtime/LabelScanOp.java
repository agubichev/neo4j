package org.neo4j.cypher.internal.compiler.v2_0.newCompiler.runtime;

import org.neo4j.kernel.impl.util.PrimitiveLongIterator;

public class LabelScanOp implements Operator {
    private final int registerIdx;
    private final Register register;
    private final PrimitiveLongIterator nodes;

    public LabelScanOp(StatementContext ctx, int registerIdx, int labelToken, Register register) {
        this.registerIdx = registerIdx;
        this.register = register;
        nodes = ctx.read().nodesGetForLabel(labelToken);
    }

    @Override
    public void open() {
    }

    @Override
    public boolean next() {
        if(!nodes.hasNext()) {
            return false;
        }

        register.setLong(registerIdx, nodes.next());
        return true;
    }

    @Override
    public void close() {
    }
}
