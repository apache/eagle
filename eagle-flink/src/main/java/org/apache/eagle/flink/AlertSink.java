package org.apache.eagle.flink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlertSink  implements SinkFunction<AlertStreamEvent> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(AlertSink.class);

    @Override
    public void invoke(AlertStreamEvent value, Context context) {
        LOG.info(value.toString());
    }
}