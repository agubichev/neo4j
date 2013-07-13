package org.neo4j.kernel.impl.disloaded;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.disloaded.operations.NodeLoad;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class StoreNodeLoader extends LifecycleAdapter implements Runnable
{
    private final NodeManager nodeManager;
    private final BlockingQueue<NodeLoad> loadFromDiskQueue;
    private ExecutorService executor;

    public StoreNodeLoader( NodeManager nodeManager, BlockingQueue<NodeLoad> loadFromDiskQueue )
    {
        this.nodeManager = nodeManager;
        this.loadFromDiskQueue = loadFromDiskQueue;
    }

    public NodeLoad load( long nodeId, Callback<NodeImpl> callBack )
    {
        return new NodeLoad( nodeId, callBack );
    }

    @Override
    public void start()
    {
        executor = Executors.newSingleThreadExecutor();
        executor.execute( this );
    }

    @Override
    public void stop()
    {
        executor.shutdown();
    }

    @Override
    public void run()
    {
        try
        {
            while ( true )
            {
                NodeLoad currentPackage = loadFromDiskQueue.take();
                System.out.println(currentPackage.getInputData() + " StoreNodeLoader found it");
                NodeImpl loadedNode = nodeManager.getNodeForProxy( currentPackage.getInputData(), null );
                currentPackage.done( loadedNode );
            }
        }
        catch ( InterruptedException e )
        {
            return;
        }
    }
}