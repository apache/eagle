package org.apache.eagle.alert.engine.spark.function;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

public class PairDataFunction implements PairFlatMapFunction<Iterable<Tuple2<Integer,Object>>, Integer, Object> {

    @Override
    public Iterable<Tuple2<Integer, Object>> call(Iterable<Tuple2<Integer, Object>> messages)
            throws Exception {
        return messages;
    }
}
