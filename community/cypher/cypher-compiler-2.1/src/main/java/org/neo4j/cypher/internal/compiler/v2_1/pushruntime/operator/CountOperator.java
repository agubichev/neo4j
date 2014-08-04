package org.neo4j.cypher.internal.compiler.v2_1.pushruntime.operator;

import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import org.neo4j.cypher.internal.compiler.v2_1.pushruntime.OperatorPush;
import org.neo4j.cypher.internal.compiler.v2_1.runtime.Expression;

/**
 * Created by andrey on 27/07/14.
 */
public class CountOperator extends OperatorPush {
    @Child
    private OperatorPush producer;

    //@Child
    //private Expression inputExpr;
    private OperatorPush consumer;

    private final FrameSlot countSlot;
    private final FrameSlot[] slots;

    private int count;

    public CountOperator(OperatorPush producer, FrameSlot countSlot){
        this.countSlot = countSlot;
        this.producer = producer;
        this.slots =  new FrameSlot[]{countSlot};
        this.count = 0;
    }

    @Override
    public FrameSlot[] getSlotsWrittenTo() {
        return slots;
    }

    @Override
    public void produce(VirtualFrame frame) {
        this.count = 0;
        producer.produce(frame);
        frame.setLong(countSlot, count);
        consumer.consume(frame);
    }

    @Override
    public void consume(VirtualFrame frame) {
        count++;
    }

    @Override
    public void setConsumer(OperatorPush op) {
        this.consumer = op;
    }
}
