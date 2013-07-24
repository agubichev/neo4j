/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphdb;

import org.junit.Test;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.fail;
import static org.neo4j.graphdb.NodeFacadeMethods.ALL_NODE_FACADE_METHODS;

public class MandatoryTransactionsForNodeFacadeTests
{
    @Test
    public void shouldRequireTransactionsWhenCallingMethodsOnNodeFacade() throws Exception
    {
        GraphDatabaseService graphDatabaseService = new TestGraphDatabaseFactory().newImpermanentDatabase();
        Node node = createNode( graphDatabaseService );

        for ( NodeFacadeMethod nodeFacadeMethod : ALL_NODE_FACADE_METHODS )
        {
            try
            {
                nodeFacadeMethod.call( node );

                fail( "Transactions are mandatory, also for reads: " + nodeFacadeMethod );
            }
            catch ( NotInTransactionException e )
            {

            }
        }
    }

    private Node createNode( GraphDatabaseService graphDatabaseService )
    {
        Transaction transaction = graphDatabaseService.beginTx();
        try
        {
            return graphDatabaseService.createNode();
        }
        finally
        {
            transaction.finish();
        }
    }
}