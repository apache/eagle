package org.apache.eagle.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class SiddhiCEPOp extends KeyedProcessFunction<Long, StreamEvent, AlertStreamEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(SiddhiCEPOp.class);
    private transient SiddhiPolicyHandler handler;

    @Override
    public void open(Configuration parameters) throws Exception{
        handler = new SiddhiPolicyHandler(createDefinition("sampleStream_1"), 0);
        PolicyDefinition policyDefinition = MockSampleMetadataFactory.createSingleMetricSamplePolicy();
        PolicyHandlerContext context = new PolicyHandlerContext();
        context.setPolicyDefinition(policyDefinition);
        context.setPolicyCounter(new MyStreamCounter());
        context.setPolicyEvaluator(new PolicyGroupEvaluatorImpl("evalutorId"));
        handler.prepare(context);
    }

    @Override
    public void processElement(StreamEvent value, Context ctx, Collector<AlertStreamEvent> out) throws Exception {
        handler.send(value, new org.apache.eagle.flink.Collector<AlertStreamEvent>(){
            @Override
            public void emit(AlertStreamEvent o) {
                out.collect(o);
            }
        });
    }

    private Map<String, StreamDefinition> createDefinition(String... streamIds) {
        Map<String, StreamDefinition> sds = new HashMap<>();
        for (String streamId : streamIds) {
            // construct StreamDefinition
            StreamDefinition sd = MockSampleMetadataFactory.createSampleStreamDefinition(streamId);
            sds.put(streamId, sd);
        }
        return sds;
    }

    @Override
    public void close() throws Exception {
        handler.close();
    }
}
