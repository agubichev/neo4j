/*
 * Copyright (C) 2012 Neo Technology
 * All rights reserved
 */
package org.neo4j.cypher.internal.compilator.v2_0;

public interface Result {
    String[] columns();

    int getInt(String key);
    String getString(String key);
    String getObject(String key);
}
