/*
 * Copyright (C) 2012 Neo Technology
 * All rights reserved
 */
package org.neo4j.cypher.internal.compilator.v2_0;

import org.neo4j.kernel.api.Statement;

public interface Operation {
    void accept(Visitor visitor, Statement statement) throws Exception;
}
