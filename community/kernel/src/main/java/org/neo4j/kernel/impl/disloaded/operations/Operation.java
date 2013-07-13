/*
 * Copyright (C) 2012 Neo Technology
 * All rights reserved
 */
package org.neo4j.kernel.impl.disloaded.operations;

public interface Operation<INPUT,RESULT>
{
    INPUT getInputData();
    void callMe(RESULT result);
}
