package org.apache.eagle.alert.engine;

public interface StreamCounter {
    void incr(String scopeName);

    void incrBy(String scopeName, int length);

    void scope(String scopeName);
}
