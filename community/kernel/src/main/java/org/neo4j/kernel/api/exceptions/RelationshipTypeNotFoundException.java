package org.neo4j.kernel.api.exceptions;

public class RelationshipTypeNotFoundException extends KernelException
{
    public RelationshipTypeNotFoundException( String message, Exception cause )
    {
        super( Status.Schema.NoSuchRelationshipType, cause, message );
    }
}
