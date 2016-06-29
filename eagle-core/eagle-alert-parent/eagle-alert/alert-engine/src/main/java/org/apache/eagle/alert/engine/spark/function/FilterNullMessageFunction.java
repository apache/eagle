package org.apache.eagle.alert.engine.spark.function;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.List;

public class FilterNullMessageFunction implements Function<Iterable<Tuple2<Integer, Object>>, Boolean> {

    @Override
    public Boolean call(Iterable<Tuple2<Integer, Object>> tuple2s) throws Exception {
        return tuple2s != null;
    }
}
