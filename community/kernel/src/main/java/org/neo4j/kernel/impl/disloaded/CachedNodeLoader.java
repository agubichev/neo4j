package org.neo4j.kernel.impl.disloaded;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.disloaded.operations.NodeLoad;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;


public class CachedNodeLoader extends LifecycleAdapter implements Runnable
{
    private final NodeManager nodeManager;
    private final BlockingQueue<NodeLoad> packageQueue;
    private final BlockingQueue<NodeLoad> loadFromDiskQueue;
    private ExecutorService executor;

    public CachedNodeLoader( NodeManager nodeManager, BlockingQueue<NodeLoad> packageQueue,
                             BlockingQueue<NodeLoad> loadFromDiskQueue )
    {
        this.nodeManager = nodeManager;
        this.packageQueue = packageQueue;
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
                NodeLoad currentPackage = packageQueue.take();
                NodeImpl loadedNode = nodeManager.getNodeIfCached( currentPackage.getInputData() );

                if ( loadedNode != null )
                {
                    System.out.println(currentPackage.getInputData() + " CachedNodeLoader found it");
                    currentPackage.done( loadedNode );
                }
                else
                {
                    System.out.println(currentPackage.getInputData() + " CachedNodeLoader didn't find it - passing the buck");
                    loadFromDiskQueue.put( currentPackage );
                }
            }
        }
        catch ( InterruptedException e )
        {
            return;
        }
    }
}