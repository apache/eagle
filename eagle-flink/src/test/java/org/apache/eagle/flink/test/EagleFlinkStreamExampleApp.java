package org.apache.eagle.flink.test;

import org.apache.eagle.flink.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

public class EagleFlinkStreamExampleApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // prepare stream definition and policy
        StreamDefinition inStreamDef = MockSampleMetadataFactory.createInStreamDef("sampleStream_1");
        String policy = MockSampleMetadataFactory.createPolicy();
        StreamDefinition outStreamDef = MockSampleMetadataFactory.createInStreamDef("outputStream");

        DataStream<StreamEvent> source = env
                .addSource(new StreamEventSource())
                .name("eagle-events");

        DataStream<AlertStreamEvent> alerts = source
                .keyBy(StreamEvent::getKey)
                .process(new SiddhiPolicyFlinkProcessor(inStreamDef, policy, outStreamDef))
                .name("eagle-alert-engine");

        alerts.addSink(new AlertSink())
                .name("eagle-alert-publisher");

        env.execute("Eagle Alert Engine");

    }
}
