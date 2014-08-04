package org.neo4j.cypher.internal.compiler.v2_1.pushruntime.operator;

import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameUtil;
import com.oracle.truffle.api.frame.VirtualFrame;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cypher.internal.compiler.v2_1.pushruntime.OperatorPush;
import org.neo4j.cypher.internal.compiler.v2_1.runtime.Expression;
import org.neo4j.cypher.internal.compiler.v2_1.runtime.Operator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Stack;

/**
 * Created by andrey on 26/07/14.
 */
public class BFSOperator extends OperatorPush {
    @Child
    private OperatorPush producer;
    @Child
    private Expression relTypeId;

    private OperatorPush consumer;

    private final int min;
    private final Integer max;

    private final FrameSlot fromSlot;
    private final FrameSlot relSlot;
    private final FrameSlot toSlot;

    private final FrameSlot[] slots;

    private final Direction direction;

    public BFSOperator( OperatorPush producer, int min, Integer max,
                                    Expression relTypeId, FrameSlot fromSlot, FrameSlot relSlot,
                                    FrameSlot toSlot, Direction direction ) {
        this.producer = producer;
        this.min = min;
        this.max = max;
        this.relTypeId = relTypeId;
        this.fromSlot = fromSlot;
        this.relSlot = relSlot;
        this.toSlot = toSlot;
        this.direction = direction;
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

    private void printPath(long[] path){
        System.out.print("[");
        for (int i = 0; i < path.length; i++)
            System.out.print(path[i] + " ");
        System.out.println("]");
    }
    @Override
    public void consume(VirtualFrame frame) {
        GraphDatabaseService graph = getGraph( frame );
        Statement statement = getStatement( frame );
        int typeId = relTypeId.executeToIntegerSafe( frame );
        Stack<long[]> stack = new Stack<long[]>();
        long nodeId = FrameUtil.getLongSafe(frame, fromSlot);
        stack.push(new long[] { nodeId });
       // System.out.println("BFS consume " + nodeId);
        while (!stack.isEmpty()) {
        //    System.out.println("stack non empty");
            long[] rels = {};

            while (rels.length - 1 < min) {
                rels = stack.pop();
                //printPath(rels);

                if (max == null || rels.length - 1 < max) {
                    PrimitiveLongIterator it;
                    try {
                        it = statement.readOperations().nodeGetRelationships(
                                rels[0], direction, typeId
                        );
                    } catch (EntityNotFoundException e) {
                        throw new RuntimeException(e);
                    }

                    while (it.hasNext()) {
                        long relId = it.next();
                        boolean contains = false;
                        for (int i = 1; i < rels.length; ++i) {
                            if (rels[i] == relId) {
                                contains = true;
                                break;
                            }
                        }
                        if (contains) {
                            continue;
                        }

                        long[] newRels = Arrays.copyOf(rels, rels.length + 1);
                        newRels[rels.length] = relId;

                        long newLastNodeId;
                        if (direction == Direction.OUTGOING) {
                            newLastNodeId = graph.getRelationshipById(relId).getEndNode().getId();
                        } else {
                            newLastNodeId = graph.getRelationshipById(relId).getStartNode().getId();
                        }
                        newRels[0] = newLastNodeId;
                       // rels = newRels;
                       // if (newRels.)
                        stack.push(newRels);
                    }
                }
            }
            frame.setObject( relSlot, Arrays.copyOfRange( rels, 1, rels.length ) );
            frame.setLong( toSlot, rels[0] );
           // stack.pop();
           // System.out.println("calling consume with path of len "+ rels.length);
            consumer.consume(frame);
        }
    }

    @Override
    public void setConsumer(OperatorPush op) {
        this.consumer = op;
    }
}
