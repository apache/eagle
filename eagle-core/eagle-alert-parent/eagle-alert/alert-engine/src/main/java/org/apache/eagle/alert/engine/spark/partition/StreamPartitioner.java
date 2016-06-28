package org.apache.eagle.alert.engine.spark.partition;

import org.apache.spark.Partitioner;
/*
//使用partitionBy重分区
scala> var rdd2 = rdd1.partitionBy(new org.apache.spark.HashPartitioner(2))
        rdd2: org.apache.spark.rdd.RDD[(Int, String)] = ShuffledRDD[25] at partitionBy at :23

        scala> rdd2.partitions.size
        res23: Int = 2*/

public class StreamPartitioner extends Partitioner {
    private int numParts;

    public StreamPartitioner() {

    }

    public StreamPartitioner(int numParts) {
        this.numParts = numParts;
    }

    @Override
    public int numPartitions() {
        return numParts;
    }

    @Override
    public int getPartition(Object key) {

      /*  String domain = new Java.net.URL(key.toString).getHost();
        int code = domain.hashCode % numPartitions;
        if (code < 0) {
            return code + this.numPartitions();
        } else {
            return code;
        }*/
        return 0;
    }

}
