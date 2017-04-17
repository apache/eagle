package org.apache.eagle.alert.engine.spark.model;

import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.HashMap;
import java.util.Map;

public class StateInstance {
    private static volatile Accumulator instance = null;
    private static Map<String, Accumulator> instanceMap = new HashMap<>();

    public static Accumulator getInstance(JavaSparkContext jsc, String accumName, AccumulatorParam clazz) {
        if (instanceMap.containsKey(accumName) && instanceMap.get(accumName) != null) {
            return instanceMap.get(accumName);
        } else {
            synchronized (StateInstance.class) {
                instance = jsc.sc().accumulator(new HashMap<>(), accumName, clazz);
                instanceMap.put(accumName, instance);
            }
            return instance;
        }
    }
}
