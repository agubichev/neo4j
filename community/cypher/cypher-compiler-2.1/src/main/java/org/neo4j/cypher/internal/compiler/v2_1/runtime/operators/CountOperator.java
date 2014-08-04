package org.neo4j.cypher.internal.compiler.v2_1.runtime.operators;

import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import org.neo4j.cypher.internal.compiler.v2_1.runtime.Operator;

/**
 * Andrey Gubichev, 27/07/14.
 */
public class CountOperator extends Operator {
    @Child
    private Operator input;

    private final FrameSlot countSlot;
    private final FrameSlot[] slots;

    private int count;

    public CountOperator(Operator input, FrameSlot countSlot){
        this.input = input;
        this.countSlot = countSlot;
        this.slots =  new FrameSlot[]{countSlot};
    }

    @Override
    public FrameSlot[] getSlotsWrittenTo() {
        return slots;
    }

    @Override
    public void open(VirtualFrame frame) {
        this.count = 0;
        input.open(frame);
    }

    @Override
    public boolean next(VirtualFrame frame) {
        while (input.next(frame)){
            count++;
        }
        frame.setLong(countSlot, count);
        System.out.println("count = "+count);
        return false;
    }

    @Override
    public void close(VirtualFrame frame) {
        input.close(frame);
    }
}
