package org.neo4j.cypher.internal.compiler.v2_1.pushruntime;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.NodeInfo;
import com.oracle.truffle.api.nodes.NodeUtil;
import org.neo4j.cypher.internal.compiler.v2_1.pushruntime.operator.MaterializeOperator;
import org.neo4j.cypher.internal.compiler.v2_1.runtime.Operator;

import java.util.ArrayList;

/**
 * Created by andrey on 23/07/14.
 */
@NodeInfo(shortName = "root")
public class RootNode extends com.oracle.truffle.api.nodes.RootNode {
    @Child
    private MaterializeOperator input;
    private final int columns;
    private final FrameSlot[] paramSlots;
    private final OperatorPush unitializedOperator;

    public RootNode(OperatorPush operator, FrameDescriptor descriptor, int columns, FrameSlot[] paramSlots) {
        super(null, descriptor);
        this.input = new MaterializeOperator(operator, descriptor, paramSlots, columns);
        this.columns = columns;
        this.paramSlots = paramSlots;
        this.unitializedOperator = NodeUtil.cloneNode(operator);
    }

    @Override
    public Object execute( VirtualFrame frame )
    {
        input.produce(frame);
        ArrayList<Object> result = input.getResults();
        if ( CompilerDirectives.inInterpreter()) {
            System.out.println("in interpreter");
            reportLoopCount( result.size() );
        }
        return result.toArray();
    }

    @Override
    public boolean isSplittable() {
        return true;
    }

    @Override
    public RootNode split() {
        return new RootNode(NodeUtil.cloneNode(unitializedOperator), getFrameDescriptor().shallowCopy(), columns, paramSlots);
    }
}
