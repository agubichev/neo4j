package org.neo4j.cypher.internal.compiler.v2_1.pushruntime.operator;

import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.NodeInfo;
import org.neo4j.cypher.internal.compiler.v2_1.pushruntime.OperatorPush;
import org.neo4j.cypher.internal.compiler.v2_1.runtime.Expression;

/**
 * Created by andrey on 24/07/14.
 */
@NodeInfo(shortName = "projection")
public class ProjectionOperator extends OperatorPush {
    private OperatorPush consumer;
    @Child
    private OperatorPush producer;
    @Child
    private Expression expression;

    public ProjectionOperator(OperatorPush producer, Expression expression){
        this.producer = producer;
        this.expression = expression;
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
        expression.executeAndIgnore( frame );
        consumer.consume(frame);
    }

    @Override
    public void setConsumer(OperatorPush op) {
        this.consumer = op;
    }
}
