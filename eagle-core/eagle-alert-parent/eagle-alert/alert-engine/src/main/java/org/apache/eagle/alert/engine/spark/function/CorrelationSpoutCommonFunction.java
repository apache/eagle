package org.apache.eagle.alert.engine.spark.function;

import org.apache.eagle.alert.engine.spark.broadcast.JavaWordBlacklist;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Time;

import java.util.List;

public class CorrelationSpoutCommonFunction implements Function2<JavaPairRDD<String, String>, Time, JavaPairRDD<String, String>> {


    @Override
    public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd, Time v2) throws Exception {
        final Broadcast<List<String>> blacklist = JavaWordBlacklist.getInstance(new JavaSparkContext(rdd.context()));
        System.out.println(blacklist.getValue().get(2));
        System.out.println(rdd.keys());
        System.out.println(rdd.values());
        return null;
    }
}
