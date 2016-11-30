package org.apache.eagle.alert.engine;

public class SparkCountMetric implements StreamCounter {
    @Override
    public void incr(String scopeName) {
        // TODO: 11/30/16  use accumulator
    }

    @Override
    public void incrBy(String scopeName, int length) {
        // TODO: 11/30/16
    }

    @Override
    public void scope(String scopeName) {
        // TODO: 11/30/16
    }
}
