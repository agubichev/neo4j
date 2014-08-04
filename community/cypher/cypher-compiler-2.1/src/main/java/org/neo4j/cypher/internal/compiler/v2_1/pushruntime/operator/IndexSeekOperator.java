package org.neo4j.cypher.internal.compiler.v2_1.pushruntime.operator;

import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.NodeInfo;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cypher.internal.compiler.v2_1.pushruntime.OperatorPush;
import org.neo4j.cypher.internal.compiler.v2_1.runtime.Expression;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;

/**
 * Created by andrey on 25/07/14.
 */
@NodeInfo(shortName = "indexseek")
public class IndexSeekOperator extends OperatorPush {
    private OperatorPush consumer;
    @Child
    private Expression expr;
    private final FrameSlot[] slots;
    private final FrameSlot slot;
    private final IndexDescriptor descriptor;

    public IndexSeekOperator( Expression expr, FrameSlot slot, int labelId, int propertyKeyId){
        this.expr = expr;
        this.slot = slot;
        this.slots = new FrameSlot[] { slot };
        this.descriptor = new IndexDescriptor( labelId, propertyKeyId );
    }

    @Override
    public FrameSlot[] getSlotsWrittenTo() {
        return slots;
    }

    @Override
    public void produce(VirtualFrame frame) {
        PrimitiveLongIterator it;
        ReadOperations ops = getStatement( frame ).readOperations();
        try
        {
            it = ops.nodesGetFromIndexLookup( descriptor, expr.execute( frame ) );
        }
        catch ( IndexNotFoundKernelException e )
        {
            throw new RuntimeException( e );
        }
        while ( it.hasNext() )
        {
            frame.setLong( slot, it.next() );
            consumer.consume(frame);
        }
    }

    @Override
    public void consume(VirtualFrame frame) {
    }

    @Override
    public void setConsumer(OperatorPush op) {
        this.consumer = op;
    }
}
