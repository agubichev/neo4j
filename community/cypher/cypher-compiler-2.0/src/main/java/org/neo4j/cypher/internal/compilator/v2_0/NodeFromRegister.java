package org.neo4j.cypher.internal.compilator.v2_0;

import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

public class NodeFromRegister implements Expression<Long> {
    private final int slotIndex;

    public NodeFromRegister(int slotIndex) {
        this.slotIndex = slotIndex;
    }

    @Override
    public Long execute(Register register, Statement statement) throws EntityNotFoundException {
        return register.getNode(slotIndex);
    }
}
