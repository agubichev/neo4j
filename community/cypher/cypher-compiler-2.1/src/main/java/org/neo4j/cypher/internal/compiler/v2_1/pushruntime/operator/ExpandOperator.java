package org.neo4j.cypher.internal.compiler.v2_1.pushruntime.operator;

import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameUtil;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.NodeInfo;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cypher.internal.compiler.v2_1.pushruntime.OperatorPush;
import org.neo4j.cypher.internal.compiler.v2_1.runtime.Expression;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by andrey on 24/07/14.
 */
@NodeInfo(shortName = "expand")
public class ExpandOperator extends OperatorPush {
    @Child
    private OperatorPush producer;
    //@Child
    private OperatorPush consumer;
    @Child
    private Expression relTypeId;

    private final FrameSlot fromSlot;
    private final FrameSlot relSlot;
    private final FrameSlot toSlot;
    private final FrameSlot[] slots;

    private final Direction direction;

    public ExpandOperator(OperatorPush producer, Expression relTypeId, FrameSlot fromSlot, FrameSlot toSlot, FrameSlot relSlot, Direction dir){
        this.fromSlot = fromSlot;
        this.relSlot = relSlot;
        this.toSlot = toSlot;
        this.relTypeId = relTypeId;
        this.producer = producer;
        this.direction = dir;
        ArrayList<FrameSlot> slots = new ArrayList<>( Arrays.asList(producer.getSlotsWrittenTo()));
        slots.add( relSlot );
        this.slots = slots.toArray( new FrameSlot[slots.size()] );
    }

    @Override
    public FrameSlot[] getSlotsWrittenTo() {
        return slots;
    }

    @Override
    public void produce(VirtualFrame frame) {
        producer.produce(frame);
    }

    @Override
    public void consume(VirtualFrame frame) {

        Statement statement = getStatement( frame );
        int typeId = relTypeId.executeToIntegerSafe( frame );
        long nodeId = FrameUtil.getLongSafe( frame, fromSlot );
        GraphDatabaseService graph = getGraph( frame );
       // System.out.println("Expand consume " + nodeId);

        PrimitiveLongIterator it;
        try {
            it = statement.readOperations().nodeGetRelationships(nodeId, direction, typeId);
        }
        catch ( EntityNotFoundException e )
        {
            throw new RuntimeException( e );
        }

        while (it.hasNext()) {
                long relId = it.next();
                Relationship relationship = graph.getRelationshipById(relId);
                frame.setLong(relSlot, relId);
                long endNodeId;
                if (direction == Direction.INCOMING) {
                    endNodeId = relationship.getStartNode().getId();
                } else {
                    endNodeId = relationship.getEndNode().getId();
                }
                frame.setLong( toSlot, endNodeId );
                consumer.consume(frame);
        }
    }

    @Override
    public void setConsumer(OperatorPush op) {
        this.consumer = op;
    }

}
