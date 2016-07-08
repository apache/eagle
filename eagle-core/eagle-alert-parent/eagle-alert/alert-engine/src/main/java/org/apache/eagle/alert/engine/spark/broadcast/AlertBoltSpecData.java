package org.apache.eagle.alert.engine.spark.broadcast;

import org.apache.eagle.alert.coordination.model.AlertBoltSpec;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class AlertBoltSpecData {
    private static volatile Broadcast<AlertBoltSpec> instance = null;

    public static Broadcast<AlertBoltSpec> getInstance(JavaSparkContext jsc, final AlertBoltSpec meta) {
        if (instance == null) {
            synchronized (AlertBoltSpecData.class) {
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
