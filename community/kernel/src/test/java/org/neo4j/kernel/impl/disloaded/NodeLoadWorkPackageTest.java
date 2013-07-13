package org.neo4j.kernel.impl.disloaded;

import org.junit.Test;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.disloaded.operations.NodeLoad;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


public class NodeLoadWorkPackageTest
{
    @Test
    public void shouldCallbackOnRequest() throws Exception
    {
        // Given
        Callback<NodeImpl> nodeRetrievedCallback = mock( Callback.class );

        NodeLoad nodeLoadPackage = new NodeLoad( 0l, nodeRetrievedCallback );
        NodeImpl daNode = new NodeImpl( 0 );

        // When
        nodeLoadPackage.done( daNode );

        // Then
        verify( nodeRetrievedCallback, times( 1 ) ).callMe( daNode );
    }
    
    @Test
    public void shouldLoadFrom() throws Exception
    {
        
    }
}
