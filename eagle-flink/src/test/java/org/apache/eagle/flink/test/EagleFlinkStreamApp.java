package org.apache.eagle.flink.test;

import org.apache.eagle.flink.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EagleFlinkStreamApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StreamEvent> source = env
                .addSource(new StreamEventSource())
                .name("eagle-events");

        DataStream<AlertStreamEvent> alerts = source
                .keyBy(StreamEvent::getKey)
                .process(new SampleSiddhiCEPOp())
                .name("eagle-alert-engine");

        alerts.addSink(new AlertSink())
                .name("eagle-alert-publisher");

        env.execute("Eagle Alert Engine");
    }
}
