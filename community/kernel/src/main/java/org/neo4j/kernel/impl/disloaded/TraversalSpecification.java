package org.neo4j.kernel.impl.disloaded;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.disloaded.operations.NodeLoad;

public class TraversalSpecification
{
    private final Iterator<Long> startNodes;
    private final RelationshipType type;
    private final Direction dir;
    private final BlockingQueue<NodeLoad> queue;


    public TraversalSpecification( Iterator<Long> startNodes, RelationshipType type, Direction dir,
                                   BlockingQueue<NodeLoad> queue )
    {
        this.startNodes = startNodes;
        this.type = type;
        this.dir = dir;
        this.queue = queue;
    }

    public RelationshipType getType()
    {
        return type;
    }

    public Iterator<NodeImpl> start()
    {
        return new BufferedIterator();
    }

    private class BufferedIterator extends PrefetchingIterator<NodeImpl>
    {
        private final Queue<NodeImpl> buffer = new LinkedBlockingQueue<NodeImpl>();

        @Override
        protected NodeImpl fetchNextOrNull()
        {
            if ( buffer.isEmpty() )
            {
                for ( int i = 0; i < 100; i++ )
                {
                    if ( startNodes.hasNext() )
                    {
                        queue.add( new NodeLoad( startNodes.next(), new Callback<NodeImpl>()
                        {
                            @Override
                            public void callMe( NodeImpl node )
                            {
                                buffer.offer( node );
                            }
                        } ) );
                    }
                    else
                    {
                        return null;
                    }
                }
            }

            return buffer.poll();
        }
    }
}
