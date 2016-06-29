package org.apache.eagle.alert.engine.spark.function;

import org.apache.eagle.alert.engine.spark.partition.StreamPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

public class TransFormFunction implements Function<JavaPairRDD<Integer, Object>, JavaPairRDD<Integer, Object>> {
    private int numOfRouter;
    public TransFormFunction(int numOfRouter){
        this.numOfRouter = numOfRouter;
    }

    @Override
    public JavaPairRDD<Integer, Object> call(JavaPairRDD<Integer, Object> v1) throws Exception {


return v1.repartition(numOfRouter);
     //   return  v1.partitionBy(new StreamPartitioner(numOfRouter));
    }
   /* @Override
    public JavaRDD<Object> call(JavaRDD<List<Tuple2<Object, Object>>> v1) throws Exception {
     *//*   v1.d
          v1.fold().keyBy(new Function<List<Tuple2<Object,Object>>, Object>() {
            @Override
            public Object call(List<Tuple2<Object, Object>> v1) throws Exception {

                return null;
            }
        }).partitionBy(new StreamPartitioner(10));
        return null;*//*
        return null;
    }*/
}
