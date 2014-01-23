package org.neo4j.cypher.internal.compiler.v2_0.newCompiler.runtime;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;

import java.util.Iterator;

public class StatementContext {
    private final Statement statement;
    private final GraphDatabaseService graph;

    public StatementContext(Statement statement, GraphDatabaseService graph) {
        this.statement = statement;
        this.graph = graph;
    }

    public ReadOperations read() {
        return statement.readOperations();
    }

    public PrimitiveLongIterator FAKEgetAllNodes() {
        final Iterator<Node> allNodes = graph.getAllNodes().iterator();
        return new PrimitiveLongIterator() {
            @Override
            public boolean hasNext() {
                return allNodes.hasNext();
            }

            @Override
            public long next() {
                return allNodes.next().getId();
            }
        };
    }

    public PrimitiveLongIterator FAKEgetNodesRelatedBy(long nodeId, Direction direction) {
        final Node nodeById = graph.getNodeById(nodeId);
        final Iterator<Relationship> relationships = nodeById.getRelationships(direction).iterator();
        return new PrimitiveLongIterator() {
            @Override
            public boolean hasNext() {
                return relationships.hasNext();
            }

            @Override
            public long next() {
                return relationships.next().getOtherNode(nodeById).getId();
            }
        };
    }



}
