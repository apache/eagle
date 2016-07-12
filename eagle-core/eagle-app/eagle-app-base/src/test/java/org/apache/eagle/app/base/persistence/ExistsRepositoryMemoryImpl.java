package org.apache.eagle.app.base.persistence;

import org.apache.eagle.app.base.persistence.annotation.Persistence;
import org.apache.eagle.app.base.persistence.memory.Memory;

@Persistence(Memory.class)
public class ExistsRepositoryMemoryImpl implements ExistsRepository {
    @Override
    public boolean exists() {
        return true;
    }
}