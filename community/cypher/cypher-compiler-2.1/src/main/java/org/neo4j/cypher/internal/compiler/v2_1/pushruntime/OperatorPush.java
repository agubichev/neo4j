package org.neo4j.cypher.internal.compiler.v2_1.pushruntime;

import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import org.neo4j.cypher.internal.compiler.v2_1.runtime.Node;

/**
 * Created by andrey on 22/07/14.
 */
public abstract class OperatorPush extends Node {
    public abstract FrameSlot[] getSlotsWrittenTo();

    public abstract void produce( VirtualFrame frame );

    public abstract void consume(VirtualFrame frame);


    public abstract void setConsumer(OperatorPush op);

}
