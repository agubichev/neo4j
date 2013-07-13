package org.neo4j.kernel.impl.disloaded;

import java.util.concurrent.ArrayBlockingQueue;

import org.junit.Test;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.disloaded.operations.NodeLoad;
import org.neo4j.test.TestGraphDatabaseFactory;

public class DisruptorIT
{
    @Test(timeout = 3000)
    public void stuff() throws Exception
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        NodeManager nodeManager = db.getDependencyResolver().resolveDependency( NodeManager.class );

        ArrayBlockingQueue<NodeLoad> loadCachedNodes = new ArrayBlockingQueue<>( 100 );
        ArrayBlockingQueue<NodeLoad> loadStoreNodes = new ArrayBlockingQueue<>( 100 );
        CachedNodeLoader cachedNodeLoader = new CachedNodeLoader( nodeManager, loadCachedNodes, loadStoreNodes );
        StoreNodeLoader storeNodeLoader = new StoreNodeLoader( nodeManager, loadStoreNodes );

        NodeLoad workToDo = new NodeLoad( 0, new Callback<NodeImpl>()
        {
            @Override
            public void callMe( NodeImpl node )
            {
                System.out.println( node );
            }
        } );
        NodeLoad workToDo2 = new NodeLoad( 0, new Callback<NodeImpl>()
        {
            @Override
            public void callMe( NodeImpl node )
            {
                System.out.println( node );
            }
        } );

        cachedNodeLoader.start();
        storeNodeLoader.start();

        loadCachedNodes.add( workToDo );
        Thread.sleep( 100 );
        loadCachedNodes.add( workToDo2 );

        storeNodeLoader.stop();
        cachedNodeLoader.stop();
        db.shutdown();
    }
}
