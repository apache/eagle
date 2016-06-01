package org.apache.eagle.alert.engine;

import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.task.TopologyContext;

import com.typesafe.config.Config;

public class StreamContextImpl implements StreamContext {
    private final Config config;
    private final MultiCountMetric counter;

    public StreamContextImpl(Config config, MultiCountMetric counter, TopologyContext context) {
        this.counter=counter;
        this.config = config;
    }

    @Override
    public MultiCountMetric counter() {
        return this.counter;
    }

    @Override
    public Config config() {
        return this.config;
    }
}