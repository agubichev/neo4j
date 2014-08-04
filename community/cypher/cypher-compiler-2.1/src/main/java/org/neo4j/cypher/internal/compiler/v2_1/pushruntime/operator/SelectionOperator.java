package org.neo4j.cypher.internal.compiler.v2_1.pushruntime.operator;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.NodeInfo;
import org.neo4j.cypher.internal.compiler.v2_1.pushruntime.OperatorPush;
import org.neo4j.cypher.internal.compiler.v2_1.runtime.Expression;

/**
 * Created by andrey on 25/07/14.
 */
@NodeInfo(shortName = "selection")
public class SelectionOperator extends OperatorPush{
    @Child
    private OperatorPush producer;
    //@Child
    private OperatorPush consumer;
    @Child
    private Expression predicate;

    public SelectionOperator(OperatorPush producer, Expression predicate){
        this.producer = producer;
        this.predicate = predicate;
    }
    @Override
    public FrameSlot[] getSlotsWrittenTo() {
        return producer.getSlotsWrittenTo();
    }

    @Override
    public void produce(VirtualFrame frame) {
        producer.produce(frame);
    }

    @Override
    public void consume(VirtualFrame frame) {
        int count = 0;
        if ( CompilerDirectives.inInterpreter()) {
            ++count;
        }

        if (predicate.executeToBooleanSafe( frame )) {
            if ( CompilerDirectives.inInterpreter()) {
                getRootNode().reportLoopCount( count );
            }
            consumer.consume(frame);
        }
    }

    @Override
    public void setConsumer(OperatorPush op) {
        this.consumer = op;
    }
}
