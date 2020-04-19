package org.apache.eagle.flink;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class SiddhiCEPOp extends KeyedProcessFunction<Long, StreamEvent, AlertPublishEvent> {
    @Override
    public void processElement(StreamEvent value, Context ctx, Collector<AlertPublishEvent> out) throws Exception {
        if(value.data[0].equals(100)) {
            AlertPublishEvent event = new AlertPublishEvent();
            out.collect(event);
        }
    }
}
