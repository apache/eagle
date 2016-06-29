package org.apache.eagle.alert.engine.spark.function;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.List;

public class FilterMessageFunction implements Function<List<Tuple2<Object, Object>>, Boolean> {

    @Override
    public Boolean call(List<Tuple2<Object, Object>> tuple2s) throws Exception {
        return tuple2s != null;
    }


}
