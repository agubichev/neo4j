package org.neo4j.cypher.internal.compiler.v2_1.pushruntime.operator;

import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.NodeInfo;
import org.neo4j.cypher.internal.compiler.v2_1.pushruntime.OperatorPush;

/**
 * Created by andrey on 25/07/14.
 */
@NodeInfo(shortName = "limit")
public class LimitOperator extends OperatorPush{
    @Child
    private OperatorPush producer;
    @Child
    private OperatorPush consumer;

    Long limit;

    public LimitOperator(OperatorPush producer, Long limit){
        this.producer = producer;
        this.limit = limit;
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
        long count = 0;
        while (count < limit) {
            count++;
            consumer.consume(frame);
        }
    }

    @Override
    public void setConsumer(OperatorPush op) {
        this.consumer = op;
    }
}
