package org.apache.eagle.alert.engine.spark.broadcast;

import org.apache.eagle.alert.coordination.model.RouterSpec;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;


public class RouterSpecData {

    private static volatile Broadcast<RouterSpec> instance = null;

    public static Broadcast<RouterSpec> getInstance(JavaSparkContext jsc, final RouterSpec meta) {
        if (instance == null) {
            synchronized (RouterSpecData.class) {
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
