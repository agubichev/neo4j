package org.neo4j.kernel.impl.disloaded.operations;

import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.disloaded.Callback;

public class NodeLoad implements Operation<Long, NodeImpl>
{
    private final long nodeId;
    private final Callback<NodeImpl> callback;

    public NodeLoad( long nodeId, Callback<NodeImpl> callback )
    {
        this.nodeId = nodeId;
        this.callback = callback;
    }

    @Override
    public Long getInputData()
    {
        return nodeId;
    }

    @Override
    public void callMe( NodeImpl node )
    {
        callback.callMe( node );
    }
}
