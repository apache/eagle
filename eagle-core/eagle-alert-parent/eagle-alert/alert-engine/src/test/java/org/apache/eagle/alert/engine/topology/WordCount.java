package org.apache.eagle.alert.engine.topology;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

public class WordCount implements Serializable {
    @Test
    public void testTopologyRun() {


        // Create DataFrame representing the stream of input lines from connection to localhost:9999
        SparkSession spark = SparkSession
                .builder().master("local[4]")
                .appName("JavaStructuredNetworkWordCount")
                .getOrCreate();
        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();

// Split the lines into words
        Dataset<String> words = lines
                .as(Encoders.STRING())
                .flatMap(
                        new FlatMapFunction<String, String>() {
                            @Override
                            public Iterator<String> call(String x) {
                                return Arrays.asList(x.split(" ")).iterator();
                            }
                        }, Encoders.STRING());

// Generate running word count
        Dataset<Row> wordCounts = words.groupBy("value").count();
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();
        query.explain();
        query.awaitTermination();
    }

}
