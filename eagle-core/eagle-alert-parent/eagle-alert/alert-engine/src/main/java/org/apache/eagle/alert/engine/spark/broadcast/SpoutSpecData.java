package org.apache.eagle.alert.engine.spark.broadcast;

import org.apache.eagle.alert.coordination.model.SpoutSpec;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class SpoutSpecData {
    private static volatile Broadcast<SpoutSpec> instance = null;

    public static Broadcast<SpoutSpec> getInstance(JavaSparkContext jsc, final SpoutSpec meta) {
        if (instance == null) {
            synchronized (SpoutSpecData.class) {
                if (instance == null) {
                    instance = jsc.broadcast(meta);
                }
            }
        }else{
            instance = jsc.broadcast(meta);
        }
        return instance;
    }
}
