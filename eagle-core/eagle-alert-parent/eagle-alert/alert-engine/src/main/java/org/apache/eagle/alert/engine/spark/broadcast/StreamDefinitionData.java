package org.apache.eagle.alert.engine.spark.broadcast;

import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Map;

public class StreamDefinitionData {
    private static volatile Broadcast<Map<String, StreamDefinition>> instance = null;

    public static Broadcast<Map<String, StreamDefinition>> getInstance(JavaSparkContext jsc, Map<String, StreamDefinition> sds ) {
        if (instance == null) {
            synchronized (StreamDefinitionData.class) {
                if (instance == null) {
                    instance = jsc.broadcast(sds);
                }
            }
        }else{
            instance = jsc.broadcast(sds);
        }
        return instance;
    }
}