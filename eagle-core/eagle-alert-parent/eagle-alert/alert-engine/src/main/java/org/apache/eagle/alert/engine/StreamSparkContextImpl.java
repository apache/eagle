package org.apache.eagle.alert.engine;

import com.typesafe.config.Config;

public class StreamSparkContextImpl implements StreamContext {

    private final Config config;
    private final StreamCounter counter;

    public StreamSparkContextImpl(Config config) {
        this.counter = new SparkCountMetric();
        this.config = config;
    }

    @Override
    public StreamCounter counter() {
        return counter;
    }

    @Override
    public Config config() {
        return config;
    }
}
