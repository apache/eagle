package org.apache.eagle.alert.engine.spark.partition;

import org.apache.spark.Partitioner;

public class StreamRoutePartitioner extends Partitioner {
    private int numParts;

    public StreamRoutePartitioner(int numParts) {
        this.numParts = numParts;
    }

    @Override
    public int numPartitions() {
        return numParts;
    }

    @Override
    public int getPartition(Object routeIndex) {
        return (Integer) routeIndex;
    }

}
