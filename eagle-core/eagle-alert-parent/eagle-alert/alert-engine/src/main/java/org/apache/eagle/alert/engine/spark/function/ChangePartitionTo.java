package org.apache.eagle.alert.engine.spark.function;

import org.apache.eagle.alert.engine.spark.partition.StreamRoutePartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class ChangePartitionTo implements Function<JavaPairRDD<Integer, Object>, JavaPairRDD<Integer, Object>> {
    private int numParts;// can be dynamic set like executor in storm

    public ChangePartitionTo(int numParts) {
        this.numParts = numParts;
    }

    @Override
    public JavaPairRDD<Integer, Object> call(JavaPairRDD<Integer, Object> rdd) throws Exception {
        return rdd.partitionBy(new StreamRoutePartitioner(numParts));
    }
}
