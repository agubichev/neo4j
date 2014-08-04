package org.neo4j.cypher.internal.compiler.v2_1.pushruntime.operator;

import com.oracle.truffle.api.CompilerAsserts;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameUtil;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.NodeInfo;
import org.neo4j.cypher.internal.compiler.v2_1.pushruntime.OperatorPush;

import java.util.*;

/**
 * Created by andrey on 25/07/14.
 */
@NodeInfo(shortName = "hashjoinnode")
public class HashJoinNodeOperator extends OperatorPush {
    @Child
    OperatorPush leftProducer;
    @Child
    OperatorPush rightProducer;

    OperatorPush consumer;
    private final FrameSlot joinSlot;
    private final FrameSlot[] slots;
    private boolean leftside;

    public HashJoinNodeOperator(OperatorPush leftProducer, OperatorPush rightProducer, FrameSlot joinSlot){
        this.leftProducer = leftProducer;
        this.rightProducer = rightProducer;
        this.joinSlot = joinSlot;
        ArrayList<FrameSlot> slots = new ArrayList<>( Arrays.asList(leftProducer.getSlotsWrittenTo()));
        for (FrameSlot slot: rightProducer.getSlotsWrittenTo()) {
            if (slots.contains( slot )) {
                slots.add( slot );
            }
        }
        this.slots = slots.toArray( new FrameSlot[slots.size()] );
        this.leftside = true;
    }


    @Override
    public FrameSlot[] getSlotsWrittenTo() {
        return slots;
    }

    @Override
    public void produce(VirtualFrame frame) {
        leftside = true;
        leftProducer.produce(frame);
        leftside = false;
        rightProducer.produce(frame);
    }

    @ExplodeLoop
    private long[] getLeftProducerValues( VirtualFrame frame ) {
        CompilerAsserts.compilationConstant(slots.length);

        FrameSlot[] slots = this.leftProducer.getSlotsWrittenTo();
        long[] row = new long[slots.length];
        for (int i = 0; i < slots.length; ++i) {
            FrameSlot slot = slots[i];
            switch (slot.getKind()) {
                case Long:
                    row[i++] = FrameUtil.getLongSafe( frame, slot );
                    break;
                case Int:
                    row[i++] = (long) FrameUtil.getIntSafe( frame, slot );
                    break;
                case Boolean:
                    row[i++] = FrameUtil.getBooleanSafe( frame, slot ) ? 1 : 0;
                    break;
                default:
                    throw new RuntimeException( "unexpected slot type" );
            }
        }
        return row;
    }

    @ExplodeLoop
    private void putLeftValues( VirtualFrame frame, long[] values ) {
        CompilerAsserts.compilationConstant(slots.length);

        FrameSlot[] slots = this.leftProducer.getSlotsWrittenTo();
        for (int i = 0; i < slots.length; ++i) {
            FrameSlot slot = slots[i];
            long value = values[i];
            switch (slot.getKind()) {
                case Long:
                    frame.setLong( slot, value );
                    break;
                case Int:
                    frame.setInt( slot, (int) value );
                    break;
                case Boolean:
                    frame.setBoolean( slot, value > 0 );
                    break;
                default:
                    throw new RuntimeException( "unexpected slot type" );
            }
        }
    }

    @Override
    public void consume(VirtualFrame frame) {
        Map<Long, ArrayList<long[]>> map = new HashMap<>();

        if (leftside){
            long joinNodeId = FrameUtil.getLongSafe(frame, joinSlot);
            ArrayList<long[]> values;
            if (map.containsKey( joinNodeId )) {
                values = map.get( joinNodeId );
            } else {
                values = new ArrayList<>();
                map.put( joinNodeId, values );
            }
            values.add(getLeftProducerValues(frame));

        } else {
            long joinNodeId = FrameUtil.getLongSafe( frame, joinSlot );
            Iterator<long[]> it = null;
            if (map.containsKey( joinNodeId )) {
                it = map.get(joinNodeId).iterator();
                while (it.hasNext()) {
                    putLeftValues(frame, it.next());
                    consumer.consume(frame);
                }
            }
        }
    }

    @Override
    public void setConsumer(OperatorPush op) {
        this.consumer = op;
    }
}
