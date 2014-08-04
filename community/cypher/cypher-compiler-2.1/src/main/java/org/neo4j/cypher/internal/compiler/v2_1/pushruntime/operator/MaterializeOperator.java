package org.neo4j.cypher.internal.compiler.v2_1.pushruntime.operator;

import com.oracle.truffle.api.CompilerAsserts;
import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.NodeInfo;
import org.neo4j.cypher.internal.compiler.v2_1.pushruntime.OperatorPush;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by andrey on 23/07/14.
 */
@NodeInfo(shortName = "materialize")
public class MaterializeOperator extends OperatorPush {
    @Child
    private OperatorPush input;

    private ArrayList<Object> result;
    private ArrayList<Object> currentRow;
    private final FrameSlot[] paramSlots;
    private final FrameSlot projection;
    private final int columns;


    public MaterializeOperator(OperatorPush input, FrameDescriptor descriptor, FrameSlot[] paramSlots, int columns){
        this.input = input;
        this.paramSlots = paramSlots;
        this.columns = columns;
        this.projection = descriptor.findFrameSlot( "projection" );
        this.input.setConsumer(this);
        //System.out.println("Proj slot in Materialize: "+projection);
        //System.out.println("Descr size: "+descriptor.getSize());
        //List<? extends FrameSlot> d = descriptor.getSlots();

        //for (FrameSlot s: d)
        //    System.out.println(s);

    }

    @Override
    public FrameSlot[] getSlotsWrittenTo() {
        return paramSlots;
    }

    @ExplodeLoop
    private void prepareSlots(VirtualFrame frame){
        Object[] params = (Object[]) frame.getArguments()[2];
        CompilerAsserts.compilationConstant(paramSlots.length);

        for (int i = 0; i < paramSlots.length; ++i) {
            FrameSlot slot = paramSlots[i];
            Object value = params[i];
            frame.setObject( slot, value );
        }

    }

    @Override
    public void produce(VirtualFrame frame) {
        result = new ArrayList<>();
        currentRow = new ArrayList<>(columns);
        prepareSlots( frame );
        frame.setObject( projection, currentRow );

        input.produce(frame);
    }

    @Override
    public void consume(VirtualFrame frame) {
        result.add( currentRow.toArray() );
        currentRow.clear();

    }

    public ArrayList<Object> getResults(){
        return result;
    }

    public void setConsumer(OperatorPush op){

    }
}
