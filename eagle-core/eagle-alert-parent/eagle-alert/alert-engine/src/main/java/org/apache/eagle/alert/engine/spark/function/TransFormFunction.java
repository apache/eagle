package org.apache.eagle.alert.engine.spark.function;

import org.apache.eagle.alert.engine.spark.partition.StreamRoutePartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

public class TransFormFunction implements Function<JavaPairRDD<Integer, Object>, JavaPairRDD<Integer, Object>> {
    private int numOfRouter;// can be dynamic set like executor in storm

    public TransFormFunction(int numOfRouter) {
        this.numOfRouter = numOfRouter;
    }

    @Override
    public JavaPairRDD<Integer, Object> call(JavaPairRDD<Integer, Object> rdd) throws Exception {

        return rdd.partitionBy(new StreamRoutePartitioner(numOfRouter));
    }
}
