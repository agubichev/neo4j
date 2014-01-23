package org.neo4j.cypher.internal.compiler.v2_0.newCompiler.runtime;

import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;

public class ExpandToNodeOp implements Operator {
    private final StatementContext ctx;
    private final int sourceIdx;
    private final int dstIdx;
    private final Operator sourceOp;
    private final Register register;
    private final Direction dir;
    private PrimitiveLongIterator currentNodes;

    public ExpandToNodeOp(StatementContext ctx, int sourceIdx, int dstIdx, Operator sourceOp, Register register, Direction dir) {
        this.ctx = ctx;
        this.sourceIdx = sourceIdx;
        this.dstIdx = dstIdx;
        this.sourceOp = sourceOp;
        this.register = register;
        this.dir = dir;
        this.currentNodes = IteratorUtil.emptyPrimitiveLongIterator();
    }

    @Override
    public void open() {
        sourceOp.open();
    }

    @Override
    public boolean next() {
        while (!currentNodes.hasNext() && sourceOp.next()) {
            long fromNodeId = register.getLong(sourceIdx);
            currentNodes = ctx.FAKEgetNodesRelatedBy(fromNodeId, dir);
        }

        if (!currentNodes.hasNext())
            return false;

        long nextNode = currentNodes.next();
        register.setLong(dstIdx, nextNode);

        return true;
    }

    @Override
    public void close() {
        sourceOp.close();
    }
}
