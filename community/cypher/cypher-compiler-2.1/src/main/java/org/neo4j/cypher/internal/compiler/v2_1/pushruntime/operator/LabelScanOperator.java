package org.neo4j.cypher.internal.compiler.v2_1.pushruntime.operator;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.NodeInfo;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cypher.internal.compiler.v2_1.pushruntime.OperatorPush;
import org.neo4j.cypher.internal.compiler.v2_1.runtime.Expression;

/**
 * Created by andrey on 22/07/14.
 */
@NodeInfo(shortName = "nodeByLabelScanPush")
public class LabelScanOperator extends OperatorPush {
    @Child
    private Expression labelId;

    private final FrameSlot slot;
    private final FrameSlot[] slots;
    //@Child
    private OperatorPush consumer;

    public LabelScanOperator(Expression labelId, FrameSlot slot)
    {
        this.labelId = labelId;
        this.slot = slot;
        this.slots = new FrameSlot[] { slot };
    }

    @Override
    public FrameSlot[] getSlotsWrittenTo() {
        return slots;
    }

    @Override
    public void produce(VirtualFrame frame) {
        //System.out.println("LabelScan produce");
        PrimitiveLongIterator it = getStatement( frame ).readOperations().nodesGetForLabel( labelId.executeToIntegerSafe( frame ) );

        int count = 0;
        while ( it.hasNext() )
        {
            long n = it.next();
            frame.setLong( slot, n);
            if ( CompilerDirectives.inInterpreter()) {
                ++count;
                getRootNode().reportLoopCount( count );
            }
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
