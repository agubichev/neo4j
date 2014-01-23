package org.neo4j.cypher.internal.compiler.v2_0.newCompiler.runtime;


import org.neo4j.kernel.impl.util.PrimitiveLongIterator;

public class AllNodesScanOp implements Operator {

    private final PrimitiveLongIterator allNodes;
    private final Register register;
    private final int dstIdx;

    public AllNodesScanOp(StatementContext ctx, Register register, int dstIdx) {
        this.register = register;
        this.dstIdx = dstIdx;
        allNodes = ctx.FAKEgetAllNodes();
    }

    @Override
    public void open() {
    }

    @Override
    public boolean next() {
        if (!allNodes.hasNext())
            return false;

        register.setLong(dstIdx, allNodes.next());

        return true;
    }

    @Override
    public void close() {
    }
}
